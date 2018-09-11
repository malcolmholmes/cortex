package cache

import (
	"context"
	"flag"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/cortex/pkg/util"
)

var (
	droppedWriteBack = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "cache_dropped_background_writes_total",
		Help:      "Total count of dropped write backs to cache.",
	}, []string{"name"})
	queueLength = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "cache_background_queue_length",
		Help:      "Length of the cache background write queue.",
	}, []string{"name"})
)

func init() {
	prometheus.MustRegister(droppedWriteBack)
	prometheus.MustRegister(queueLength)
}

// BackgroundConfig is config for a Background Cache.
type BackgroundConfig struct {
	WriteBackGoroutines int
	WriteBackBuffer     int
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *BackgroundConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.WriteBackGoroutines, "memcache.write-back-goroutines", 10, "How many goroutines to use to write back to memcache.")
	f.IntVar(&cfg.WriteBackBuffer, "memcache.write-back-buffer", 10000, "How many chunks to buffer for background write back.")
}

type backgroundCache struct {
	Cache

	wg       sync.WaitGroup
	quit     chan struct{}
	bgWrites chan backgroundWrite
	name     string
}

type backgroundWrite struct {
	key string
	buf []byte
}

// NewBackground returns a new Cache that does stores on background goroutines.
func NewBackground(name string, cfg BackgroundConfig, cache Cache) Cache {
	c := &backgroundCache{
		Cache:    cache,
		quit:     make(chan struct{}),
		bgWrites: make(chan backgroundWrite, cfg.WriteBackBuffer),
		name:     name,
	}

	c.wg.Add(cfg.WriteBackGoroutines)
	for i := 0; i < cfg.WriteBackGoroutines; i++ {
		go c.writeBackLoop()
	}

	return c
}

// Stop the background flushing goroutines.
func (c *backgroundCache) Stop() error {
	close(c.quit)
	c.wg.Wait()

	return c.Cache.Stop()
}

// Store writes keys for the cache in the background.
func (c *backgroundCache) Store(ctx context.Context, key string, buf []byte) error {
	bgWrite := backgroundWrite{
		key: key,
		buf: buf,
	}
	select {
	case c.bgWrites <- bgWrite:
		queueLength.WithLabelValues(c.name).Inc()
	default:
		droppedWriteBack.WithLabelValues(c.name).Inc()
	}
	return nil
}

func (c *backgroundCache) writeBackLoop() {
	defer c.wg.Done()

	for {
		select {
		case bgWrite, ok := <-c.bgWrites:
			if !ok {
				return
			}
			queueLength.WithLabelValues(c.name).Dec()
			err := c.Cache.Store(context.Background(), bgWrite.key, bgWrite.buf)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error writing to cache", "err", err, "cache", c.name)
			}
		case <-c.quit:
			return
		}
	}
}
