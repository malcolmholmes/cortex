package cache

import (
	"context"
	"flag"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	droppedWriteBack = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "cache_dropped_background_writes_total",
		Help:      "Total count of dropped write backs to cache.",
	})
	queueLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "cache_background_queue_length",
		Help:      "Length of the cache background write queue.",
	})
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
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *BackgroundConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	if prefix != "" {
		prefix = prefix + "."
	}

	f.IntVar(&cfg.WriteBackGoroutines, prefix+"memcache.write-back-goroutines", 10, "How many goroutines to use to write back to memcache.")
	f.IntVar(&cfg.WriteBackBuffer, prefix+"memcache.write-back-buffer", 10000, "How many chunks to buffer for background write back.")
}

type backgroundCache struct {
	Cache

	wg       sync.WaitGroup
	quit     chan struct{}
	bgWrites chan backgroundWrite
}

type backgroundWrite struct {
	keys []string
	bufs [][]byte
}

// NewBackground returns a new Cache that does stores on background goroutines.
func NewBackground(cfg BackgroundConfig, cache Cache) Cache {
	c := &backgroundCache{
		Cache:    cache,
		quit:     make(chan struct{}),
		bgWrites: make(chan backgroundWrite, cfg.WriteBackBuffer),
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
func (c *backgroundCache) Store(ctx context.Context, keys []string, bufs [][]byte) {
	bgWrite := backgroundWrite{
		keys: keys,
		bufs: bufs,
	}
	select {
	case c.bgWrites <- bgWrite:
		queueLength.Add(float64(len(keys)))
	default:
		sp := opentracing.SpanFromContext(ctx)
		sp.LogFields(otlog.Int("dropped", len(keys)))
		droppedWriteBack.Add(float64(len(keys)))
	}
}

func (c *backgroundCache) writeBackLoop() {
	defer c.wg.Done()

	for {
		select {
		case bgWrite, ok := <-c.bgWrites:
			if !ok {
				return
			}
			queueLength.Sub(float64(len(bgWrite.keys)))
			c.Cache.Store(context.Background(), bgWrite.keys, bgWrite.bufs)

		case <-c.quit:
			return
		}
	}
}
