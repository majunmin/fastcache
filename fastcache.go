// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package fastcache

import (
	"fmt"
	"sync"
	"sync/atomic"

	xxhash "github.com/cespare/xxhash/v2"
)

const bucketsCount = 512

const chunkSize = 64 * 1024

const bucketSizeBits = 40

const genSizeBits = 64 - bucketSizeBits

const maxGen = 1<<genSizeBits - 1 // 2^24 - 1

const maxBucketSize uint64 = 1 << bucketSizeBits // 2^40

// Stats represents cache stats.
//
// Use Cache.UpdateStats for obtaining fresh stats from the cache.
type Stats struct {
	// GetCalls is the number of Get calls.
	GetCalls uint64

	// SetCalls is the number of Set calls.
	SetCalls uint64

	// Misses is the number of cache misses.
	Misses uint64

	// Collisions is the number of cache collisions.
	//
	// Usually the number of collisions must be close to zero.
	// High number of collisions suggest something wrong with cache.
	Collisions uint64

	// Corruptions is the number of detected corruptions of the cache.
	//
	// Corruptions may occur when corrupted cache is loaded from file.
	Corruptions uint64

	// EntriesCount is the current number of entries in the cache.
	EntriesCount uint64

	// BytesSize is the current size of the cache in bytes.
	BytesSize uint64

	// MaxBytesSize is the maximum allowed size of the cache in bytes (aka capacity).
	MaxBytesSize uint64

	// BigStats contains stats for GetBig/SetBig methods.
	BigStats
}

// Reset resets s, so it may be re-used again in Cache.UpdateStats.
func (s *Stats) Reset() {
	*s = Stats{}
}

// BigStats contains stats for GetBig/SetBig methods.
// 记录了 一些  Cache 方法统计信息
type BigStats struct {
	// GetBigCalls is the number of GetBig calls.
	GetBigCalls uint64

	// SetBigCalls is the number of SetBig calls.
	SetBigCalls uint64

	// TooBigKeyErrors is the number of calls to SetBig with too big key.
	TooBigKeyErrors uint64

	// InvalidMetavalueErrors is the number of calls to GetBig resulting
	// to invalid metavalue.
	InvalidMetavalueErrors uint64

	// InvalidValueLenErrors is the number of calls to GetBig resulting
	// to a chunk with invalid length.
	InvalidValueLenErrors uint64

	// InvalidValueHashErrors is the number of calls to GetBig resulting
	// to a chunk with invalid hash value.
	InvalidValueHashErrors uint64
}

func (bs *BigStats) reset() {
	atomic.StoreUint64(&bs.GetBigCalls, 0)
	atomic.StoreUint64(&bs.SetBigCalls, 0)
	atomic.StoreUint64(&bs.TooBigKeyErrors, 0)
	atomic.StoreUint64(&bs.InvalidMetavalueErrors, 0)
	atomic.StoreUint64(&bs.InvalidValueLenErrors, 0)
	atomic.StoreUint64(&bs.InvalidValueHashErrors, 0)
}

// Cache is a fast thread-safe inmemory cache optimized for big number
// of entries.
//
// It has much lower impact on GC comparing to a simple `map[string][]byte`.
//
// Use New or LoadFromFile* for creating new cache instance.
// Concurrent goroutines may call any Cache methods on the same cache instance.
//
// Call Reset when the cache is no longer needed. This reclaims the allocated
// memory.
type Cache struct {
	buckets [bucketsCount]bucket // bucketCount default 512

	bigStats BigStats
}

// New returns new cache with the given maxBytes capacity in bytes.
//
// maxBytes must be smaller than the available RAM size for the app,
// since the cache holds data in memory.
//
// If maxBytes is less than 32MB, then the minimum cache capacity is 32MB.
func New(maxBytes int) *Cache {
	if maxBytes <= 0 {
		panic(fmt.Errorf("maxBytes must be greater than 0; got %d", maxBytes))
	}
	var c Cache
	// 如果 maxByte / bucketCount 有余数的话, 把超出的部分 分填到 每一个 bucket --向上取整
	maxBucketBytes := uint64((maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] {
		c.buckets[i].Init(maxBucketBytes)
	}
	return &c
}

// Set stores (k, v) in the cache.
//
// Get must be used for reading the stored entry.
//
// The stored entry may be evicted at any time either due to cache
// overflow or due to unlikely hash collision.
// Pass higher maxBytes value to New if the added items disappear
// frequently.
//
// (k, v) entries with summary size exceeding 64KB aren't stored in the cache.
// SetBig can be used for storing entries exceeding 64KB.
//
// k and v contents may be modified after returning from Set.
func (c *Cache) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount // 这里 可以 使用  位运算:  h & (bucketsCount - 1)
	c.buckets[idx].Set(k, v, h)
}

// Get appends value by the key k to dst and returns the result.
//
// Get allocates new byte slice for the returned value if dst is nil.
//
// Get returns only values stored in c via Set.
//
// k contents may be modified after returning from Get.
func (c *Cache) Get(dst, k []byte) []byte {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	dst, _ = c.buckets[idx].Get(dst, k, h, true)
	return dst
}

// HasGet works identically to Get, but also returns whether the given key
// exists in the cache. This method makes it possible to differentiate between a
// stored nil/empty value versus and non-existing value.
func (c *Cache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return c.buckets[idx].Get(dst, k, h, true)
}

// Has returns true if entry for the given key k exists in the cache.
func (c *Cache) Has(k []byte) bool {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	_, ok := c.buckets[idx].Get(nil, k, h, false)
	return ok
}

// Del deletes value for the given k from the cache.
//
// k contents may be modified after returning from Del.
func (c *Cache) Del(k []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Del(h)
}

// Reset removes all the items from the cache.
func (c *Cache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
	c.bigStats.reset()
}

// UpdateStats adds cache stats to s.
//
// Call s.Reset before calling UpdateStats if s is re-used.
func (c *Cache) UpdateStats(s *Stats) {
	for i := range c.buckets[:] {
		c.buckets[i].UpdateStats(s)
	}
	s.GetBigCalls += atomic.LoadUint64(&c.bigStats.GetBigCalls)
	s.SetBigCalls += atomic.LoadUint64(&c.bigStats.SetBigCalls)
	s.TooBigKeyErrors += atomic.LoadUint64(&c.bigStats.TooBigKeyErrors)
	s.InvalidMetavalueErrors += atomic.LoadUint64(&c.bigStats.InvalidMetavalueErrors)
	s.InvalidValueLenErrors += atomic.LoadUint64(&c.bigStats.InvalidValueLenErrors)
	s.InvalidValueHashErrors += atomic.LoadUint64(&c.bigStats.InvalidValueHashErrors)
}

// 一个 bucket
type bucket struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of 64KB chunks.
	// 每个 块是64K, 也就是 chunkSize
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	// key: hash(key) value:  idx, value 在 chunk中的偏移量(byte) (b.gen << bucketSizeBits | idx) => 可以计算出偏移量 offset
	m map[uint64]uint64

	// idx points to chunks for writing the next (k, v) pair.
	// 用于计算下一个chunk
	idx uint64 // 下一次 要写入的 位置

	// gen is the generation of chunks.
	gen uint64 // 由于 b.chunks 是一个ringBuf, gen 记录 chunks 的循环使用次数 24bit: gen  40bit: idx

	// 一些统计信息
	getCalls    uint64
	setCalls    uint64
	misses      uint64
	collisions  uint64
	corruptions uint64
}

func (b *bucket) Init(maxBytes uint64) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize))
	}
	// 同样的,  需要的 chunk数  -- 相当于向上取整
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
	b.Reset()
}

func (b *bucket) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	for i := range chunks {
		putChunk(chunks[i])
		chunks[i] = nil
	}
	b.m = make(map[uint64]uint64)
	b.idx = 0
	b.gen = 1
	atomic.StoreUint64(&b.getCalls, 0)
	atomic.StoreUint64(&b.setCalls, 0)
	atomic.StoreUint64(&b.misses, 0)
	atomic.StoreUint64(&b.collisions, 0)
	atomic.StoreUint64(&b.corruptions, 0)
	b.mu.Unlock()
}

func (b *bucket) cleanLocked() {
	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := b.idx
	bm := b.m
	for k, v := range bm {
		// 取出 当前 key对应的 kv  的 gen & idx
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		// 下面两种场景,不需要清理
		// 1. 上一代数据 不需要清理
		// 	idx >= bIdx && gen+1 == bGen
		// 	idx >= bIdx && gen == maxGen && bGen == 1
		// 2. 本代数据, idx < bIdx 不需要清理.
		// 	idx < bIdx  && gen == bGen
		if ((gen+1 == bGen || (gen == maxGen && bGen == 1)) && idx >= bIdx) ||
			(gen == bGen && idx < bIdx) {
			continue
		}
		delete(bm, k)
	}
}

func (b *bucket) UpdateStats(s *Stats) {
	s.GetCalls += atomic.LoadUint64(&b.getCalls)
	s.SetCalls += atomic.LoadUint64(&b.setCalls)
	s.Misses += atomic.LoadUint64(&b.misses)
	s.Collisions += atomic.LoadUint64(&b.collisions)
	s.Corruptions += atomic.LoadUint64(&b.corruptions)

	b.mu.RLock()
	s.EntriesCount += uint64(len(b.m))
	bytesSize := uint64(0)
	for _, chunk := range b.chunks {
		bytesSize += uint64(cap(chunk))
	}
	s.BytesSize += bytesSize
	s.MaxBytesSize += uint64(len(b.chunks)) * chunkSize
	b.mu.RUnlock()
}

// 2byte | 2byte | nbyte
// keylen|valLen|kv
func (b *bucket) Set(k, v []byte, h uint64) {
	atomic.AddUint64(&b.setCalls, 1)
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return
	}
	// 大端存储
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v)) // 要写入的数据的长度
	if kvLen >= chunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return
	}

	// chunk
	// chunkIdx:  chunk
	// 0:[000000000000000000]  // 64K
	// 1:[000000000000000000]
	// 2:[000000000000000000]
	// 3:[000000000000000000]
	chunks := b.chunks
	needClean := false
	b.mu.Lock()
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew > chunkIdx { //当前 chunk 放不下 该 kv, 往下一个 chunk 写 或者 循环写
		if chunkIdxNew >= uint64(len(chunks)) { // 循环写
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			b.gen++                              // 循环次数
			if b.gen&((1<<genSizeBits)-1) == 0 { // b.gen&maxGen == 0, 说明 b.gen == maxGen() 溢出, 重新计数 b.gen= 1
				b.gen++ // 此处 ++ 的目的是为了 循环取余
			}
			needClean = true // 当 chunks 满了, 需要 从 头chunks[0]开始写, 需要把当前 chunk 全部清空.(清空  b.m)
		} else { // 大部分场景
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		chunks[chunkIdx] = chunks[chunkIdx][:0] // 将该chunk清空
	}
	chunk := chunks[chunkIdx]
	// 首次初始化 , chunk[i] == nil
	if chunk == nil {
		chunk = getChunk() // 性能关键点: 堆外分配内存
		chunk = chunk[:0]
	}
	// 正式写入数据
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits) // gen + idx
	b.idx = idxNew
	if needClean { // 该 chunk 原来有数据, 需要被覆盖
		// 清理 覆盖的  overflow 的 chunk(遍历 b.m,逐个 判断 清理)
		b.cleanLocked()
	}
	b.mu.Unlock()
}

func (b *bucket) Get(dst, k []byte, h uint64, returnDst bool) ([]byte, bool) {
	atomic.AddUint64(&b.getCalls, 1)
	found := false
	chunks := b.chunks
	b.mu.RLock()
	v := b.m[h]                              // 读取 value， 从中解析出  idx = v & ((1 << bucketSizeBits) - 1)  & gen = v >> bucketSizeBits
	bGen := b.gen & ((1 << genSizeBits) - 1) // 当前 bucket处于第几代: bGen
	if v > 0 {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		// gen == bGen && idx < b.idx: 在一个循环内
		// gen+1 == bGen && idx >= b.idx: 不在一个循环内,数据没有覆盖
		// gen == maxGen && bGen == 1 && idx >= b.idx: 重新开始循环,数据没有覆盖
		if gen == bGen && idx < b.idx || gen+1 == bGen && idx >= b.idx || gen == maxGen && bGen == 1 && idx >= b.idx {
			chunkIdx := idx / chunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// Corrupted data during the load from file. Just skip it.
				// 文件加载时损坏的数据,跳过
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			chunk := chunks[chunkIdx]
			idx %= chunkSize
			if idx+4 >= chunkSize {
				// Corrupted data during the load from file. Just skip it.
				// 文件加载时损坏的数据,跳过
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4
			// 文件加载时损坏的数据,跳过
			if idx+keyLen+valLen >= chunkSize {
				// Corrupted data during the load from file. Just skip it.
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			// hash 值相同, 判断 key 是否一致
			if string(k) == string(chunk[idx:idx+keyLen]) {
				// dst = chunk[idx+keyLen:idx+valLen]
				idx += keyLen
				if returnDst {
					dst = append(dst, chunk[idx:idx+valLen]...)
				}
				found = true
			} else {
				// 发生hash collision
				atomic.AddUint64(&b.collisions, 1)
			}
		}
	}
end:
	b.mu.RUnlock()
	if !found {
		atomic.AddUint64(&b.misses, 1)
	}
	return dst, found
}

func (b *bucket) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}
