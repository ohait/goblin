package trie

import (
	"sync/atomic"
)

/*
type b int32

func (b *b) Set(v bool) (changed bool) {
	if v {
		return atomic.CompareAndSwapInt32((*int32)(b), 0, 1)
	} else {
		return atomic.CompareAndSwapInt32((*int32)(b), 1, 0)
	}
}
func (b *b) Get() bool {
	return atomic.LoadInt32((*int32)(b)) == 1
}
*/

type i int64

func (i *i) Inc(amt int) int {
	return int(atomic.AddInt64((*int64)(i), int64(amt)))
}

func (i *i) Set(amt int) {
	atomic.StoreInt64((*int64)(i), int64(amt))
}

func (i *i) Get() int {
	return int(atomic.LoadInt64((*int64)(i)))
}

/*
type m[K any, V any] sync.Map

func (this *m[K, V]) Get(k K) (v V) {
	a, exists := (*sync.Map)(this).Load(k)
	if exists {
		return a.(V)
	} else {
		return
	}
}

func (this *m[K, V]) Load(k K) (v V, exists bool) {
	a, exists := (*sync.Map)(this).Load(k)
	if exists {
		return a.(V), true
	} else {
		return
	}
}

func (this *m[K, V]) GetOrStore(k K, def V) (v V) {
	a, exists := (*sync.Map)(this).LoadOrStore(k, def)
	if exists {
		return a.(V)
	} else {
		return
	}
}

func (this *m[K, V]) Delete(k K) (old V) {
	a, exists := (*sync.Map)(this).LoadAndDelete(k)
	if exists {
		return a.(V)
	} else {
		return
	}
}

func (this *m[K, V]) Range(f func(K, V) bool) {
	(*sync.Map)(this).Range(func(k, v any) bool {
		return f(k.(K), v.(V))
	})
}
*/
