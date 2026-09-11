package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Runner struct {
	cfg          Config
	httpClient   *http.Client
	failedStarts int
}

func NewRunner(cfg Config) *Runner {
	return &Runner{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

func (r *Runner) Run(ctx context.Context) (Result, string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Duration)
	defer cancel()

	var (
		mu            sync.Mutex
		clients       []*Client
		serverSamples []ServerMetrics
		clientSamples []ClientSample
	)

	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		ticker := time.NewTicker(r.cfg.SamplePeriod)
		defer ticker.Stop()
		for {
			if sm, err := r.fetchServerMetrics(ctx); err == nil {
				mu.Lock()
				serverSamples = append(serverSamples, sm)
				mu.Unlock()
			}
			mu.Lock()
			for _, c := range clients {
				clientSamples = append(clientSamples, c.Sample())
			}
			mu.Unlock()

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	if r.cfg.JoinLeaveLoop {
		r.runJoinLeave(ctx, &mu, &clients)
	} else {
		if err := r.startParticipants(ctx, &mu, &clients); err != nil {
			return Result{}, "", err
		}
		<-ctx.Done()
		if r.cfg.RampDown > 0 {
			rampDownClients(r.cfg.RampDown, clients)
		}
	}

	for _, c := range clients {
		_ = c.Close()
	}
	<-metricsDone
	time.Sleep(500 * time.Millisecond)

	if sm, err := r.fetchServerMetrics(context.Background()); err == nil {
		serverSamples = append(serverSamples, sm)
	}
	result := Summarize(r.cfg, serverSamples, clientSamples)
	result.Summary.FailedConnections = r.failedStarts
	for _, c := range clients {
		if c.ClusterError() != "" {
			result.Summary.RedirectedConnections++
		}
	}
	if urls := r.cfg.NodeURLs(); len(urls) > 1 {
		result.NodeClusters = map[string]json.RawMessage{}
		for _, u := range urls {
			if raw, err := r.fetchClusterBlock(context.Background(), u); err == nil {
				result.NodeClusters[u] = raw
				var cb struct {
					Messages struct {
						Failed int64 `json:"failed"`
					} `json:"messages"`
					Routing struct {
						Local  int64 `json:"local"`
						Remote int64 `json:"remote"`
					} `json:"routing"`
				}
				if json.Unmarshal(raw, &cb) == nil {
					result.Summary.LocalMessages += cb.Routing.Local
					result.Summary.RemoteMessages += cb.Routing.Remote
					result.Summary.RemoteMessageErrors += cb.Messages.Failed
				}
			}
		}
	}
	path, err := WriteResult(r.cfg.OutputDir, result)
	return result, path, err
}

func (r *Runner) startParticipants(ctx context.Context, mu *sync.Mutex, clients *[]*Client) error {
	publishers := r.cfg.PublisherCount()
	delay := time.Duration(0)
	if r.cfg.RampUp > 0 && r.cfg.Participants > 1 {
		delay = r.cfg.RampUp / time.Duration(r.cfg.Participants-1)
	}
	for i := 0; i < r.cfg.Participants; i++ {
		c := NewClient(i+1, r.cfg, i < publishers)
		c.serverURL = r.cfg.NodeURL(i)
		if err := c.Start(ctx); err != nil {
			// A participant may legitimately fail to connect — e.g. it was
			// handed an invalid token on purpose (the "5% invalid" mix).
			// Record it and keep going; only a run where nobody connects
			// is a hard failure.
			r.failedStarts++
			fmt.Fprintf(os.Stderr, "participant %d failed to start: %v\n", i+1, err)
			continue
		}
		mu.Lock()
		*clients = append(*clients, c)
		mu.Unlock()
		waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		_ = c.WaitConnected(waitCtx)
		cancel()
		if delay > 0 && i < r.cfg.Participants-1 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

func (r *Runner) runJoinLeave(ctx context.Context, mu *sync.Mutex, clients *[]*Client) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c := NewClient(i+1, r.cfg, i%2 == 0 || i < r.cfg.PublisherCount())
			c.serverURL = r.cfg.NodeURL(i)
			if err := c.Start(ctx); err == nil {
				mu.Lock()
				*clients = append(*clients, c)
				if len(*clients) > r.cfg.Participants {
					old := (*clients)[0]
					*clients = (*clients)[1:]
					_ = old.Close()
				}
				mu.Unlock()
				i++
			}
		}
	}
}

func rampDownClients(total time.Duration, clients []*Client) {
	if len(clients) == 0 {
		return
	}
	delay := total / time.Duration(len(clients))
	for _, c := range clients {
		_ = c.Close()
		time.Sleep(delay)
	}
}

// adminToken returns a bearer for the protected /metrics endpoint: the
// explicitly configured ADMIN_TOKEN, or one freshly minted from the shared
// secret when auth is enabled.
func (r *Runner) adminToken() string {
	if !r.cfg.Auth.Enabled {
		return ""
	}
	if r.cfg.Auth.AdminToken != "" {
		return r.cfg.Auth.AdminToken
	}
	tok, _ := r.cfg.Auth.tokenForClient(-1, "", false)
	return tok
}

// fetchClusterBlock returns the raw "cluster" object from a node's /metrics.
func (r *Runner) fetchClusterBlock(ctx context.Context, nodeURL string) (json.RawMessage, error) {
	url := strings.TrimRight(nodeURL, "/") + "/metrics.json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if tok := r.adminToken(); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("metrics endpoint returned %s", resp.Status)
	}
	var body struct {
		Cluster json.RawMessage `json:"cluster"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Cluster, nil
}

func (r *Runner) fetchServerMetrics(ctx context.Context) (ServerMetrics, error) {
	var out ServerMetrics
	url := strings.TrimRight(r.cfg.ServerURL, "/") + "/metrics.json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return out, err
	}
	if tok := r.adminToken(); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return out, fmt.Errorf("metrics endpoint returned %s", resp.Status)
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}
