package cache

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/fileutil"
	"golang.org/x/sys/unix"
)

var (
	bucketsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "diskcache_buckets_total",
		Help:      "Total count of buckets in the cache.",
	})
	bucketsInitialized = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "diskcache_buckets_initialized_total",
		Help:      "total number of buckets that have been initialized to cache a chunk",
	})
	collisionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "diskcache_collisions_total",
		Help:      "total number of collisions when storing a key in the cache",
	})
)

func init() {
	prometheus.MustRegister(bucketsTotal)
	prometheus.MustRegister(bucketsInitialized)
	prometheus.MustRegister(collisionsTotal)
}

// TODO: in the future we could cuckoo hash or linear probe.

// Buckets contain key (~50), chunks (1024) and their metadata (~100)
const bucketSize = 2048

// DiskcacheConfig for the Disk cache.
type DiskcacheConfig struct {
	Path string
	Size int
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *DiskcacheConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Path, "diskcache.path", "/var/run/chunks", "Path to file used to cache chunks.")
	f.IntVar(&cfg.Size, "diskcache.size", 1024*1024*1024, "Size of file (bytes)")
}

// Diskcache is an on-disk chunk cache.
type Diskcache struct {
	f       *os.File
	buckets uint32
	buf     []byte
	index   []indexEntry
}

type indexEntry struct {
	mtx        sync.RWMutex
	currentKey string
}

// NewDiskcache creates a new on-disk cache.
func NewDiskcache(cfg DiskcacheConfig) (*Diskcache, error) {
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

	buckets := len(buf) / bucketSize
	bucketsTotal.Set(float64(buckets)) // Report the number of buckets in the diskcache as a metric

	index := make([]indexEntry, buckets)

	return &Diskcache{
		f:       f,
		buf:     buf,
		index:   index,
		buckets: uint32(buckets),
	}, nil
}

// Stop closes the file.
func (d *Diskcache) Stop() error {
	if err := unix.Munmap(d.buf); err != nil {
		return err
	}
	return d.f.Close()
}

// FetchChunkData get chunks from the cache.
func (d *Diskcache) FetchChunkData(ctx context.Context, keys []string) (found []string, bufs [][]byte, missed []string, err error) {
	for _, key := range keys {
		buf, ok := d.fetch(key)
		if ok {
			found = append(found, key)
			bufs = append(bufs, buf)
		} else {
			missed = append(missed, key)
		}
	}
	return
}

func (d *Diskcache) fetch(key string) ([]byte, bool) {
	bucket := hash(key) % d.buckets

	d.index[bucket].mtx.RLock()
	defer d.index[bucket].mtx.RUnlock()
	if d.index[bucket].currentKey != key {
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

// StoreChunk puts a chunk into the cache.
func (d *Diskcache) StoreChunk(ctx context.Context, key string, value []byte) error {
	bucket := hash(key) % d.buckets

	d.index[bucket].mtx.Lock()
	defer d.index[bucket].mtx.Unlock()

	if d.index[bucket].currentKey == key { // If chunk is already cached return nil
		return nil
	}

	if d.index[bucket].currentKey == "" {
		bucketsInitialized.Inc()
	} else {
		collisionsTotal.Inc()
	}

	buf := d.buf[bucket*bucketSize : (bucket+1)*bucketSize]

	_, err := put(value, buf, 0)
	if err != nil {
		d.index[bucket].currentKey = ""
		return err
	}

	d.index[bucket].currentKey = key
	return nil
}

// put places a value in the buffer in the following format
// | uint64 <length of value> | value |
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
