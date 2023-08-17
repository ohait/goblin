package trie

import (
	"fmt"
	"sort"
	"sync"
)

type node[T any] struct {
	m        sync.Mutex
	children children[T]
	count    i // how many children and grand children including self
	val      *T
}

func (this *node[T]) String() string {
	out := "{"
	if this.val != nil {
		out += fmt.Sprintf("«%v»", *this.val)
	} else {
		out += "<nil>"
	}
	for _, c := range this.children {
		out += fmt.Sprintf(", %q: %v", c.ch, c.node.String())
	}
	return out + "}"
}

type child[T any] struct {
	ch   byte
	node *node[T]
}

type children[T any] []child[T]

var _ sort.Interface = children[int]{}

func (this children[T]) Len() int {
	return len(this)
}

func (this children[T]) Less(l, r int) bool {
	return this[l].ch < this[r].ch
}

func (this children[T]) Swap(l, r int) {
	this[l], this[r] = this[r], this[l]
}

func (this *node[T]) put(k []byte, val T) (old *T) {
	defer func() {
		if old == nil {
			this.count.Inc(1)
		}
	}()

	this.m.Lock()
	if len(k) == 0 {
		old = this.val
		this.val = &val
		this.m.Unlock()
		return old
	}
	char, suffix := k[0], k[1:]

	var n *node[T]
	if this.children == nil {
		this.children = make([]child[T], 0, 4)
	}
	for _, ch := range this.children {
		if ch.ch == char {
			n = ch.node
			break
		}
	}
	if n == nil {
		n = &node[T]{}
		this.children = append(this.children, child[T]{char, n})
		sort.Sort(this.children)
	}
	this.m.Unlock()

	return n.put(suffix, val)
}

func (this *node[T]) get(k []byte) *T {
	this.m.Lock()
	if len(k) == 0 {
		this.m.Unlock()
		return this.val
	}
	if len(this.children) == 0 {
		this.m.Unlock()
		return nil
	}
	char, suffix := k[0], k[1:]
	var n *node[T]
	for _, child := range this.children {
		if child.ch == char {
			n = child.node
			break
		}
	}
	this.m.Unlock()

	if n != nil {
		return n.get(suffix)
	}
	return nil
}

func (this *node[T]) delete(k []byte) (old *T) {
	defer func() {
		if old != nil {
			this.count.Inc(-1)
		}
	}()

	this.m.Lock()
	if len(k) == 0 {
		old = this.val
		this.val = nil
		this.m.Unlock()
		return old
	}
	if len(this.children) == 0 {
		this.m.Unlock()
		return nil
	}
	char, suffix := k[0], k[1:]
	var n *node[T]
	for _, child := range this.children {
		if child.ch == char {
			n = child.node
			break
		}
	}
	this.m.Unlock()

	if n != nil {
		return n.delete(suffix)
	}
	return nil
}

func (this *node[T]) range_(prefix []byte, f func(k []byte, val T) error) (err error) {
	var list = make([]child[T], len(this.children))

	this.m.Lock()
	val := this.val
	copy(list, this.children)
	this.m.Unlock()

	if val != nil {
		err := f(prefix, *this.val)
		if err != nil {
			return err
		}
	}
	for _, child := range list {
		k := append(prefix, byte(child.ch))
		err = child.node.range_(k, f)
		if err != nil {
			return err
		}
	}
	return nil
}
