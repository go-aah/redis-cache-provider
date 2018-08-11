// Copyright (c) Jeevanandam M. (https://github.com/jeevatkm)
// aahframework.org/cache/redis source code and usage is governed by a MIT style
// license that can be found in the LICENSE file.

package redis

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"aahframework.org/aah.v0/cache"
	"aahframework.org/config.v0"
	"aahframework.org/log.v0"
	"aahframework.org/test.v0/assert"
)

func TestRedisCache(t *testing.T) {
	mgr := createCacheMgr(t, "redis1", `
	cache {
		redis1 {
			provider = "redis"
			address = "localhost:6379"
		}
		static {

		}
	}
`)

	e := mgr.CreateCache(&cache.Config{Name: "cache1", ProviderName: "redis1"})
	assert.FailNowOnError(t, e, "unable to create cache")
	c := mgr.Cache("cache1")

	type sample struct {
		Name    string
		Present bool
		Value   string
	}

	testcases := []struct {
		label string
		key   string
		value interface{}
	}{
		{
			label: "Redis Cache integer",
			key:   "key1",
			value: 342348347,
		},
		{
			label: "Redis Cache float",
			key:   "key2",
			value: 0.78346374,
		},
		{
			label: "Redis Cache string",
			key:   "key3",
			value: "This is mt cache string",
		},
		{
			label: "Redis Cache map",
			key:   "key4",
			value: map[string]interface{}{"key1": 343434, "key2": "kjdhdsjkdhjs", "key3": 87235.3465},
		},
		{
			label: "Redis Cache struct",
			key:   "key5",
			value: sample{Name: "Jeeva", Present: true, Value: "redis cache provider"},
		},
	}

	err := c.Put("pre-test-key1", sample{Name: "Jeeva", Present: true, Value: "redis cache provider"}, 3*time.Second)
	assert.Equal(t, errors.New("aah/cache: gob: type not registered for interface: redis.sample"), err)

	gob.Register(map[string]interface{}{})
	gob.Register(sample{})

	for _, tc := range testcases {
		t.Run(tc.label, func(t *testing.T) {
			assert.False(t, c.Exists(tc.key))
			assert.Nil(t, c.Get(tc.key))

			err := c.Put(tc.key, tc.value, 3*time.Second)
			assert.Nil(t, err)

			v := c.Get(tc.key)
			assert.Equal(t, tc.value, v)

			c.Delete(tc.key)
			v = c.GetOrPut(tc.key, tc.value, 3*time.Second)
			assert.Equal(t, tc.value, v)
		})
	}

	c.Flush()
}

func TestRedisCacheAddAndGet(t *testing.T) {
	c := createTestCache(t, "redis1", `
	cache {
		redis1 {
			provider = "redis"
			address = "localhost:6379"
		}
	}
`, &cache.Config{Name: "addgetcache", ProviderName: "redis1"})

	for i := 0; i < 20; i++ {
		c.Put(fmt.Sprintf("key_%v", i), i, 3*time.Second)
	}

	for i := 5; i < 10; i++ {
		v := c.Get(fmt.Sprintf("key_%v", i))
		assert.Equal(t, i, v)
	}
	assert.Equal(t, "addgetcache", c.Name())
}

func TestRedisMultipleCache(t *testing.T) {
	mgr := createCacheMgr(t, "redis1", `
	cache {
		redis1 {
			provider = "redis"
			address = "localhost:6379"
		}
	}
`)

	names := []string{"testcache1", "testcache2", "testcache3"}
	for _, name := range names {
		err := mgr.CreateCache(&cache.Config{Name: name, ProviderName: "redis1"})
		assert.FailNowOnError(t, err, "unable to create cache")

		c := mgr.Cache(name)
		assert.NotNil(t, c)
		assert.Equal(t, name, c.Name())

		for i := 0; i < 20; i++ {
			c.Put(fmt.Sprintf("key_%v", i), i, 3*time.Second)
		}

		for i := 5; i < 10; i++ {
			v := c.Get(fmt.Sprintf("key_%v", i))
			assert.Equal(t, i, v)
		}
		c.Flush()

		p := mgr.Provider("redis1").(*Provider)
		assert.NotNil(t, p.Client())
	}
}

func TestRedisSlideEvictionMode(t *testing.T) {
	c := createTestCache(t, "redis1", `
	cache {
		redis1 {
			provider = "redis"
			address = "localhost:6379"
		}
	}
`, &cache.Config{Name: "addgetcache", ProviderName: "redis1", EvictionMode: cache.EvictionModeSlide})

	for i := 0; i < 20; i++ {
		c.Put(fmt.Sprintf("key_%v", i), i, 3*time.Second)
	}

	for i := 5; i < 10; i++ {
		v := c.GetOrPut(fmt.Sprintf("key_%v", i), i, 3*time.Second)
		assert.Equal(t, i, v)
	}

	assert.Equal(t, "addgetcache", c.Name())
}

func TestRedisInvalidProviderName(t *testing.T) {
	mgr := cache.NewManager()
	mgr.AddProvider("redis1", new(Provider))

	cfg, _ := config.ParseString(`cache {
		redis1 {
			provider = "myredis"
			address = "localhost:6379"
		}
	}`)
	l, _ := log.New(config.NewEmpty())
	err := mgr.InitProviders(cfg, l)
	assert.Equal(t, errors.New("aah/cache: not a vaild provider name, expected 'redis'"), err)
}

func TestRedisInvalidAddress(t *testing.T) {
	mgr := cache.NewManager()
	mgr.AddProvider("redis1", new(Provider))

	cfg, _ := config.ParseString(`cache {
		redis1 {
			provider = "redis"
			address = "localhost:637967"
		}
	}`)
	l, _ := log.New(config.NewEmpty())
	err := mgr.InitProviders(cfg, l)
	assert.Equal(t, errors.New("aah/cache: dial tcp: address 637967: invalid port"), err)
}

func TestParseTimeDuration(t *testing.T) {
	d := parseDuration("", "1m")
	assert.Equal(t, float64(1), d.Minutes())
}

func createCacheMgr(t *testing.T, name, appCfgStr string) *cache.Manager {
	mgr := cache.NewManager()
	mgr.AddProvider(name, new(Provider))

	cfg, _ := config.ParseString(appCfgStr)
	l, _ := log.New(config.NewEmpty())
	l.SetWriter(ioutil.Discard)
	err := mgr.InitProviders(cfg, l)
	assert.FailNowOnError(t, err, "unexpected")
	return mgr
}

func createTestCache(t *testing.T, name, appCfgStr string, cacheCfg *cache.Config) cache.Cache {
	mgr := createCacheMgr(t, name, appCfgStr)
	e := mgr.CreateCache(cacheCfg)
	assert.FailNowOnError(t, e, "unable to create cache")
	return mgr.Cache(cacheCfg.Name)
}
