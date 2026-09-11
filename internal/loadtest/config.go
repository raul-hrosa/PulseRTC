package loadtest

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServerURL string `json:"serverUrl"`
	// ServerURLs, when set (comma-separated --server-urls / SERVER_URLS), spreads
	// participants across several nodes. One entry behaves like
	// ServerURL. RTP still stays on whichever node owns the room — this only
	// exercises ownership / location / routing, not cross-node media.
	ServerURLs    []string      `json:"serverUrls,omitempty"`
	RoomID        string        `json:"roomId"`
	Participants  int           `json:"participants"`
	Publishers    int           `json:"publishers"`
	PublishAudio  bool          `json:"publishAudio"`
	PublishVideo  bool          `json:"publishVideo"`
	SubscribeAll  bool          `json:"subscribeAll"`
	Duration      time.Duration `json:"duration"`
	RampUp        time.Duration `json:"rampUp"`
	RampDown      time.Duration `json:"rampDown"`
	Scenario      string        `json:"scenario"`
	OutputDir     string        `json:"outputDir"`
	SamplePeriod  time.Duration `json:"samplePeriod"`
	JoinLeaveLoop bool          `json:"joinLeaveLoop"`
	Auth          AuthOptions   `json:"auth"`
}

func DefaultConfig() Config {
	return Config{
		ServerURL:    envString("SERVER_URL", "http://localhost:8090"),
		ServerURLs:   splitList(os.Getenv("SERVER_URLS")),
		RoomID:       envString("ROOM_ID", "load-test"),
		Participants: envInt("PARTICIPANTS", 2),
		Publishers:   envInt("PUBLISHERS", -1),
		PublishAudio: envBool("PUBLISH_AUDIO", true),
		PublishVideo: envBool("PUBLISH_VIDEO", true),
		SubscribeAll: envBool("SUBSCRIBE_ALL", true),
		Duration:     envDuration("DURATION", 5*time.Minute),
		RampUp:       envDuration("RAMP_UP", 0),
		RampDown:     envDuration("RAMP_DOWN", 0),
		Scenario:     envString("SCENARIO", "custom"),
		OutputDir:    envString("OUTPUT_DIR", "benchmarks"),
		SamplePeriod: envDuration("SAMPLE_PERIOD", time.Second),
		Auth: AuthOptions{
			Enabled:      envBool("AUTH_ENABLED", false),
			Secret:       envString("JWT_SECRET", ""),
			Issuer:       envString("JWT_ISSUER", "pulsertc"),
			Audience:     envString("JWT_AUDIENCE", "pulsertc"),
			InvalidRatio: envFloat("AUTH_INVALID_RATIO", 0),
			AdminToken:   envString("ADMIN_TOKEN", ""),
		},
	}
}

func envFloat(key string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func ParseFlags(args []string) (Config, error) {
	cfg := DefaultConfig()
	if scenario := scenarioArg(args); scenario != "" {
		cfg.Scenario = scenario
	}
	if preset, ok := ScenarioPreset(cfg.Scenario); ok {
		preset.apply(&cfg)
	}

	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.StringVar(&cfg.ServerURL, "server-url", cfg.ServerURL, "PulseRTC server URL")
	serverURLs := fs.String("server-urls", strings.Join(cfg.ServerURLs, ","), "comma-separated node URLs; participants are spread round-robin")
	fs.StringVar(&cfg.RoomID, "room", cfg.RoomID, "room id")
	fs.IntVar(&cfg.Participants, "participants", cfg.Participants, "number of participants")
	fs.IntVar(&cfg.Publishers, "publishers", cfg.Publishers, "number of publishing participants; default follows scenario or participants")
	fs.BoolVar(&cfg.PublishAudio, "publish-audio", cfg.PublishAudio, "publish synthetic audio")
	fs.BoolVar(&cfg.PublishVideo, "publish-video", cfg.PublishVideo, "publish synthetic video")
	fs.BoolVar(&cfg.SubscribeAll, "subscribe-all", cfg.SubscribeAll, "subscribe to every remote publication")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "benchmark duration")
	fs.DurationVar(&cfg.RampUp, "ramp-up", cfg.RampUp, "time to progressively add participants")
	fs.DurationVar(&cfg.RampDown, "ramp-down", cfg.RampDown, "time to progressively remove participants")
	fs.StringVar(&cfg.Scenario, "scenario", cfg.Scenario, "scenario name")
	fs.StringVar(&cfg.OutputDir, "output-dir", cfg.OutputDir, "directory for JSON results")
	fs.DurationVar(&cfg.SamplePeriod, "sample-period", cfg.SamplePeriod, "metrics sample interval")
	fs.BoolVar(&cfg.Auth.Enabled, "auth", cfg.Auth.Enabled, "authenticate each participant with a JWT")
	fs.StringVar(&cfg.Auth.Secret, "auth-secret", cfg.Auth.Secret, "HS256 signing secret shared with the server")
	fs.Float64Var(&cfg.Auth.InvalidRatio, "auth-invalid-ratio", cfg.Auth.InvalidRatio, "fraction of participants given an invalid token (0-1)")
	fs.StringVar(&cfg.Auth.AdminToken, "admin-token", cfg.Auth.AdminToken, "bearer token for the protected /metrics endpoint")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.ServerURLs = splitList(*serverURLs)
	return cfg, cfg.Normalize()
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimRight(strings.TrimSpace(p), "/"); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// NodeURLs returns every target node URL (at least one).
func (c Config) NodeURLs() []string {
	if len(c.ServerURLs) > 0 {
		return c.ServerURLs
	}
	return []string{c.ServerURL}
}

// NodeURL returns the target node for participant index i (round-robin).
func (c Config) NodeURL(i int) string {
	urls := c.NodeURLs()
	if i < 0 {
		return urls[0]
	}
	return urls[i%len(urls)]
}

func scenarioArg(args []string) string {
	for i, arg := range args {
		if arg == "--scenario" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--scenario=") {
			return strings.TrimPrefix(arg, "--scenario=")
		}
	}
	return ""
}

func (c *Config) Normalize() error {
	if c.ServerURL == "" {
		return fmt.Errorf("server URL is required")
	}
	if c.RoomID == "" {
		return fmt.Errorf("room id is required")
	}
	if c.Participants <= 0 {
		return fmt.Errorf("participants must be > 0")
	}
	if c.Publishers < 0 {
		c.Publishers = c.Participants
	}
	if c.Publishers > c.Participants {
		return fmt.Errorf("publishers cannot exceed participants")
	}
	if c.Duration <= 0 {
		return fmt.Errorf("duration must be > 0")
	}
	if c.SamplePeriod <= 0 {
		return fmt.Errorf("sample period must be > 0")
	}
	return nil
}

func (c Config) PublisherCount() int {
	if c.Publishers < 0 {
		return c.Participants
	}
	return c.Publishers
}

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
