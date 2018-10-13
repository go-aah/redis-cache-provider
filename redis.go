// Copyright (c) Jeevanandam M. (https://github.com/jeevatkm)
// Source code and usage is governed by a MIT style
// license that can be found in the LICENSE file.

package redis // import "aahframe.work/cache/provider/redis"

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"aahframe.work/cache"
	"aahframe.work/config"
	"aahframe.work/log"
	"github.com/go-redis/redis"
)

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Provider and its exported methods
//______________________________________________________________________________

// Provider struct represents the Redis cache provider.
type Provider struct {
	name       string
	logger     log.Loggerer
	cfg        *cache.Config
	appCfg     *config.Config
	client     *redis.Client
	clientOpts *redis.Options
}

var _ cache.Provider = (*Provider)(nil)

// Init method initializes the Redis cache provider.
func (p *Provider) Init(providerName string, appCfg *config.Config, logger log.Loggerer) error {
	p.name = providerName
	p.appCfg = appCfg
	p.logger = logger

	cfgPrefix := "cache." + p.name + "."
	if strings.ToLower(p.appCfg.StringDefault(cfgPrefix+"provider", "")) != "redis" {
		return fmt.Errorf("aah/cache: not a vaild provider name, expected 'redis'")
	}

	p.clientOpts = &redis.Options{
		Network:            p.appCfg.StringDefault(cfgPrefix+"network", "tcp"),
		Addr:               p.appCfg.StringDefault(cfgPrefix+"address", ":6379"),
		Password:           p.appCfg.StringDefault(cfgPrefix+"password", ""),
		DB:                 p.appCfg.IntDefault(cfgPrefix+"db", 0),
		PoolSize:           p.appCfg.IntDefault(cfgPrefix+"pool_size", 10*runtime.NumCPU()),
		DialTimeout:        parseDuration(p.appCfg.StringDefault(cfgPrefix+"timeout.connect", "5s"), "5s"),
		ReadTimeout:        parseDuration(p.appCfg.StringDefault(cfgPrefix+"timeout.read", "3s"), "3s"),
		WriteTimeout:       parseDuration(p.appCfg.StringDefault(cfgPrefix+"timeout.write", "3s"), "3s"),
		PoolTimeout:        parseDuration(p.appCfg.StringDefault(cfgPrefix+"timeout.pool", "3s"), "3s"),
		IdleTimeout:        parseDuration(p.appCfg.StringDefault(cfgPrefix+"timeout.idle", "5m"), "5m"),
		IdleCheckFrequency: parseDuration(p.appCfg.StringDefault(cfgPrefix+"idle_check_interval", "1m"), "1m"),
		MinRetryBackoff:    parseDuration(p.appCfg.StringDefault(cfgPrefix+"retry_backoff.min", "8ms"), "8ms"),
		MaxRetryBackoff:    parseDuration(p.appCfg.StringDefault(cfgPrefix+"retry_backoff.max", "512ms"), "512ms"),
	}

	p.client = redis.NewClient(p.clientOpts)
	if _, err := p.client.Ping().Result(); err != nil {
		return fmt.Errorf("aah/cache/%s: %s", p.name, err)
	}

	gob.Register(entry{})
	p.logger.Infof("aah/cache/provider: %s connected successfully with %s", p.name, p.clientOpts.Addr)

	return nil
}

// Create method creates new Redis cache with given options.
func (p *Provider) Create(cfg *cache.Config) (cache.Cache, error) {
	p.cfg = cfg
	r := &redisCache{
		keyPrefix: p.cfg.Name + "-",
		p:         p,
	}
	return r, nil
}

// Client method returns underlying redis client. So that aah user could perform
// cache provider specific features.
func (p *Provider) Client() *redis.Client {
	return p.client
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// redisCache struct implements `cache.Cache` interface.
//______________________________________________________________________________

type redisCache struct {
	keyPrefix string
	p         *Provider
}

var _ cache.Cache = (*redisCache)(nil)

// Name method returns the cache store name.
func (r *redisCache) Name() string {
	return r.p.cfg.Name
}

// Get method returns the cached entry for given key if it exists otherwise nil.
// Method uses `gob.Decoder` to unmarshal cache value from bytes.
func (r *redisCache) Get(k string) interface{} {
	k = r.keyPrefix + k
	v, err := r.p.client.Get(k).Bytes()
	if err != nil {
		if notacacheMiss(err) != nil {
			r.p.logger.Errorf("aah/cache/%s: key(%s) %v", r.Name(), k[len(r.keyPrefix):], err)
		}
		return nil
	}

	var e entry
	err = gob.NewDecoder(bytes.NewBuffer(v)).Decode(&e)
	if err != nil {
		r.p.logger.Errorf("aah/cache/%s: %v", r.Name(), err)
		return nil
	}
	if r.p.cfg.EvictionMode == cache.EvictionModeSlide {
		if err = r.p.client.Expire(k, e.D).Err(); err != nil {
			r.p.logger.Errorf("aah/cache/%s: key(%s) %v", r.Name(), k[len(r.keyPrefix):], err)
		}
	}

	return e.V
}

// GetOrPut method returns the cached entry for the given key if it exists otherwise
// it puts the new entry into cache store and returns the value.
func (r *redisCache) GetOrPut(k string, v interface{}, d time.Duration) (interface{}, error) {
	ev := r.Get(k)
	if ev == nil {
		if err := r.Put(k, v, d); err != nil {
			return nil, err
		}
		return v, nil
	}
	return ev, nil
}

// Put method adds the cache entry with specified expiration. Returns error
// if cache entry exists. Method uses `gob.Encoder` to marshal cache value into bytes.
func (r *redisCache) Put(k string, v interface{}, d time.Duration) error {
	e := entry{D: d, V: v}
	buf := acquireBuffer()
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(e); err != nil {
		return fmt.Errorf("aah/cache/%s: %v", r.Name(), err)
	}

	cmd := r.p.client.Set(r.keyPrefix+k, buf.Bytes(), d)
	releaseBuffer(buf)
	return cmd.Err()
}

// Delete method deletes the cache entry from cache store.
func (r *redisCache) Delete(k string) error {
	if err := r.p.client.Del(r.keyPrefix + k).Err(); notacacheMiss(err) != nil {
		return fmt.Errorf("aah/cache/%s: key(%s) %v", r.Name(), k, err)
	}
	return nil
}

// Exists method checks given key exists in cache store and its not expried.
func (r *redisCache) Exists(k string) bool {
	result, err := r.p.client.Exists(r.keyPrefix + k).Result()
	if err != nil {
		r.p.logger.Errorf("aah/cache/%s: key(%s) %v", r.Name(), k, err)
		return false
	}
	return result == 1
}

// Flush methods flushes(deletes) all the cache entries from cache.
func (r *redisCache) Flush() error {
	if err := r.p.client.FlushDB().Err(); err != nil {
		return fmt.Errorf("aah/cache/%s: %v", r.Name(), err)
	}
	return nil
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Helper methods
//______________________________________________________________________________

type entry struct {
	D time.Duration
	V interface{}
}

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func acquireBuffer() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func releaseBuffer(b *bytes.Buffer) {
	if b != nil {
		b.Reset()
		bufPool.Put(b)
	}
}

func parseDuration(v, f string) time.Duration {
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	d, _ := time.ParseDuration(f)
	return d
}

func notacacheMiss(err error) error {
	if err != nil && err.Error() == "redis: nil" {
		return nil
	}
	return err
}
