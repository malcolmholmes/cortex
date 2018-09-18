package cache

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/tsdb/fileutil"
	"golang.org/x/sys/unix"
)

var (
	bucketsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "diskcache_buckets_total",
		Help:      "Total count of buckets in the cache.",
	})
	bucketsInitialized = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "diskcache_added_new_total",
		Help:      "total number of entries added to the cache",
	})
	collisionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "diskcache_evicted_total",
		Help:      "total number entries evicted from the cache",
	})

	globalCache *Diskcache
	once        sync.Once
)

// TODO: in the future we could cuckoo hash or linear probe.

const (
	// Buckets contain chunks (1024) and their metadata (~100)
	bucketSize = 2048

	// Total number of mutexes shared by the disk cache index
	numMutexes = 1000
)

type diskcacheIndex struct {
	entries      []string
	entryMutexes []sync.RWMutex
}

func newDiskcacheIndex(buckets uint32) *diskcacheIndex {
	return &diskcacheIndex{
		entries:      make([]string, buckets),
		entryMutexes: make([]sync.RWMutex, numMutexes),
	}
}

// DiskcacheConfig for the Disk cache.
type DiskcacheConfig struct {
	Path    string
	Size    int
	Timeout time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *DiskcacheConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Path, "diskcache.path", "/var/run/chunks", "Path to file used to cache chunks.")
	f.IntVar(&cfg.Size, "diskcache.size", 1024*1024*1024, "Size of file (bytes)")
	f.DurationVar(&cfg.Timeout, "diskcache.timeout", 100*time.Millisecond, "Maximum time to wait before giving up on a diskcache request")
}

// Diskcache is an on-disk chunk cache.
type Diskcache struct {
	f       *os.File
	buckets uint32
	buf     []byte
	index   *diskcacheIndex
	timeout time.Duration
}

// NewDiskcache creates a new on-disk cache.
func NewDiskcache(cfg DiskcacheConfig) (*Diskcache, error) {
	once.Do(func() {
		var err error
		globalCache, err = newDiskcache(cfg)
		if err != nil {
			panic(fmt.Sprintf("unable to initialize diskcache, %v", err))
		}
	})
	return globalCache, nil
}

func newDiskcache(cfg DiskcacheConfig) (*Diskcache, error) {
	f, err := os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "open")
	}

	if err := fileutil.Preallocate(f, int64(cfg.Size), true); err != nil {
		return nil, errors.Wrap(err, "preallocate")
	}

	info, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat")
	}

	buf, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}

	buckets := uint32(len(buf) / bucketSize)
	bucketsTotal.Set(float64(buckets)) // Report the number of buckets in the diskcache as a metric

	return &Diskcache{
		f:       f,
		buckets: buckets,
		buf:     buf,
		index:   newDiskcacheIndex(buckets),
		timeout: cfg.Timeout,
	}, nil
}

// Stop closes the file.
func (d *Diskcache) Stop() error {
	if err := unix.Munmap(d.buf); err != nil {
		return err
	}
	return d.f.Close()
}

// Fetch get chunks from the cache.
func (d *Diskcache) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missed []string, err error) {
	timeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	for _, key := range keys {
		select {
		case <-timeout.Done():
			missed = append(missed, key)
		default:
			buf, ok := d.fetch(key)
			if ok {
				found = append(found, key)
				bufs = append(bufs, buf)
			} else {
				missed = append(missed, key)
			}
		}
	}
	return
}

func (d *Diskcache) fetch(key string) ([]byte, bool) {
	bucket := hash(key) % d.buckets
	shard := bucket % numMutexes // Get the index of the mutex associated with this bucket
	d.index.entryMutexes[shard].RLock()
	defer d.index.entryMutexes[shard].RUnlock()
	if d.index.entries[bucket] != key {
		return nil, false
	}

	buf := d.buf[bucket*bucketSize : (bucket+1)*bucketSize]
	existingValue, _, ok := get(buf, 0)
	if !ok {
		return nil, false
	}

	result := make([]byte, len(existingValue), len(existingValue))
	copy(result, existingValue)
	return result, true
}

// Store puts a chunk into the cache.
func (d *Diskcache) Store(ctx context.Context, key string, value []byte) error {
	bucket := hash(key) % d.buckets
	shard := bucket % numMutexes // Get the index of the mutex associated with this bucket
	d.index.entryMutexes[shard].Lock()
	defer d.index.entryMutexes[shard].Unlock()
	if d.index.entries[bucket] == key { // If chunk is already cached return nil
		return nil
	}

	if d.index.entries[bucket] == "" {
		bucketsInitialized.Inc()
	} else {
		collisionsTotal.Inc()
	}

	buf := d.buf[bucket*bucketSize : (bucket+1)*bucketSize]
	_, err := put(value, buf, 0)
	if err != nil {
		d.index.entries[bucket] = ""
		return err
	}

	d.index.entries[bucket] = key
	return nil
}

// put places a value in the buffer in the following format
// |u int64 <length of key> | key | uint64 <length of value> | value |
func put(value []byte, buf []byte, n int) (int, error) {
	if len(value)+n+4 > len(buf) {
		return 0, errors.Wrap(fmt.Errorf("value too big: %d > %d", len(value), len(buf)), "put")
	}
	m := binary.PutUvarint(buf[n:], uint64(len(value)))
	copy(buf[n+m:], value)
	return len(value) + n + m, nil
}

func get(buf []byte, n int) ([]byte, int, bool) {
	size, m := binary.Uvarint(buf[n:])
	end := n + m + int(size)
	if end > len(buf) {
		return nil, 0, false
	}
	return buf[n+m : end], end, true
}

func hash(key string) uint32 {
	h := fnv.New32()
	h.Write([]byte(key))
	return h.Sum32()
}
