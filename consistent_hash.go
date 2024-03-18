package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type HashCircle []uint32

func (h HashCircle) Len() int           { return len(h) }
func (h HashCircle) Less(i, j int) bool { return h[i] < h[j] }
func (h HashCircle) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

type ConsistentHash struct {
	circle           HashCircle
	virtualNodeCount int
	nodeHashMap      map[uint32]string
	mutex            sync.RWMutex // 添加读写锁
}

func NewConsistentHash(virtualNodeCount int) *ConsistentHash {
	return &ConsistentHash{
		circle:           HashCircle{},
		virtualNodeCount: virtualNodeCount,
		nodeHashMap:      make(map[uint32]string),
	}
}

func (h *ConsistentHash) AddNode(node string) {
	if h == nil {
		panic("ConsistentHash is nil")
	}
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.circle == nil {
		h.circle = make(HashCircle, 0)
	}
	if h.nodeHashMap == nil {
		h.nodeHashMap = make(map[uint32]string)
	}

	for i := 0; i < h.virtualNodeCount; i++ {
		virtualNode := node + strconv.Itoa(i)
		hashValue := crc32.ChecksumIEEE([]byte(virtualNode))
		h.circle = append(h.circle, hashValue)
		if _, exists := h.nodeHashMap[hashValue]; !exists {
			h.nodeHashMap[hashValue] = node
		}
	}
	sort.Sort(h.circle)
}

func (h *ConsistentHash) RemoveNode(node string) {
	if h == nil || h.circle == nil || h.nodeHashMap == nil {
		return // 防止空指针异常
	}
	h.mutex.Lock()         // 在修改数据之前加锁
	defer h.mutex.Unlock() // 使用defer语句保证锁会解除
	for i := 0; i < h.virtualNodeCount; i++ {
		virtualNode := node + strconv.Itoa(i)
		hashValue := crc32.ChecksumIEEE([]byte(virtualNode))
		idx := sort.Search(len(h.circle), func(i int) bool { return h.circle[i] >= hashValue })
		if idx < len(h.circle) && h.circle[idx] == hashValue {
			h.circle = append(h.circle[:idx], h.circle[idx+1:]...)
			delete(h.nodeHashMap, hashValue)
		}
	}
	sort.Sort(h.circle)
}

func (h *ConsistentHash) GetNode(key string) string {
	if h == nil || h.circle == nil || h.nodeHashMap == nil {
		return "" // Guard against nil pointer dereferences
	}
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	if len(h.circle) == 0 {
		return ""
	}
	hashValue := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(h.circle), func(i int) bool { return h.circle[i] >= hashValue })
	if idx == len(h.circle) {
		idx = 0
	}
	node, ok := h.nodeHashMap[h.circle[idx]]
	if !ok {
		return "" // Guard against missing node in the hash map
	}
	return node
}
