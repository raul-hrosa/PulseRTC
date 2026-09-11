package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisClient is the single component through which the cluster talks to Redis.
// Nothing else in the codebase imports go-redis — signaling and SFU only
// see the ClusterState / locator interfaces.
type redisClient struct {
	rdb    *redis.Client
	prefix string
	// onError is bumped on every failed command so /metrics can surface Redis
	// health problems.
	onError func()
}

// compareDeleteScript deletes a key only if its current value matches ARGV[1].
// Running it server-side makes the check-and-delete atomic, closing the
// "A reads owner, B re-claims after expiry, A deletes B's claim" race.
var compareDeleteScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// recoverOwnerScript is the atomic compare-and-recover.
// KEYS[1] = room ownership key, KEYS[2] = ownership generation key.
// ARGV[1] = oldOwner, ARGV[2] = newOwner.
// Returns {code, generation}:
//
//	0 = room key missing / nothing to recover
//	1 = ownership moved oldOwner→newOwner, generation incremented
//	2 = newOwner already owns it (idempotent, generation untouched)
//	3 = current owner is someone else (conflict, ownership untouched)
var recoverOwnerScript = redis.NewScript(`
local cur = redis.call("GET", KEYS[1])
if not cur then return {0, 0} end
local gen = tonumber(redis.call("GET", KEYS[2]) or "0")
if cur == ARGV[2] then return {2, gen} end
if cur ~= ARGV[1] then return {3, gen} end
gen = gen + 1
redis.call("SET", KEYS[1], ARGV[2])
redis.call("SET", KEYS[2], gen)
return {1, gen}
`)

func newRedisClient(cfg RedisConfig) *redisClient {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	opts := &redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	}
	return &redisClient{rdb: redis.NewClient(opts), prefix: cfg.prefix()}
}

// newRedisClientFrom wraps an already-built *redis.Client (tests use miniredis).
func newRedisClientFrom(rdb *redis.Client, prefix string) *redisClient {
	if prefix == "" {
		prefix = "pulsertc"
	}
	return &redisClient{rdb: rdb, prefix: prefix}
}

func (c *redisClient) fail(err error) error {
	if err != nil && !errors.Is(err, redis.Nil) && c.onError != nil {
		c.onError()
	}
	return err
}

func (c *redisClient) key(parts ...string) string {
	k := c.prefix
	for _, p := range parts {
		k += ":" + p
	}
	return k
}

func (c *redisClient) Ping(ctx context.Context) error {
	return c.fail(c.rdb.Ping(ctx).Err())
}

func (c *redisClient) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, c.fail(err)
	}
	return v, true, nil
}

func (c *redisClient) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	return c.fail(c.rdb.Set(ctx, key, val, ttl).Err())
}

func (c *redisClient) SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	ok, err := c.rdb.SetNX(ctx, key, val, ttl).Result()
	return ok, c.fail(err)
}

func (c *redisClient) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return c.fail(c.rdb.Del(ctx, keys...).Err())
}

func (c *redisClient) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.fail(c.rdb.Expire(ctx, key, ttl).Err())
}

// CompareDelete runs compareDeleteScript; returns true when the key was deleted.
func (c *redisClient) CompareDelete(ctx context.Context, key, want string) (bool, error) {
	res, err := compareDeleteScript.Run(ctx, c.rdb, []string{key}, want).Int()
	if err != nil {
		return false, c.fail(err)
	}
	return res == 1, nil
}

// RecoverOwner runs recoverOwnerScript; see its doc for the code meanings.
func (c *redisClient) RecoverOwner(ctx context.Context, roomKey, genKey, oldOwner, newOwner string) (code int, gen int64, err error) {
	res, e := recoverOwnerScript.Run(ctx, c.rdb, []string{roomKey, genKey}, oldOwner, newOwner).Slice()
	if e != nil {
		return 0, 0, c.fail(e)
	}
	if len(res) >= 1 {
		if v, ok := res[0].(int64); ok {
			code = int(v)
		}
	}
	if len(res) >= 2 {
		if v, ok := res[1].(int64); ok {
			gen = v
		}
	}
	return code, gen, nil
}

// ScanKeys returns every key matching pattern (used for the small node/room
// enumerations behind /metrics and /internal/nodes — never on the media path).
func (c *redisClient) ScanKeys(ctx context.Context, pattern string) ([]string, error) {
	var out []string
	iter := c.rdb.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		out = append(out, iter.Val())
	}
	return out, c.fail(iter.Err())
}

func (c *redisClient) MGet(ctx context.Context, keys ...string) ([]any, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	vals, err := c.rdb.MGet(ctx, keys...).Result()
	return vals, c.fail(err)
}

func (c *redisClient) Close() error { return c.rdb.Close() }
