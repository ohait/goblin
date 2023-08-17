package trie

import (
	"errors"
)

/*
Memory efficient storage for [string,T] in pure go
*/

type Trie[T any] struct {
	root node[T]
}

func (this *Trie[T]) String() string {
	return this.root.String()
}

func (this *Trie[T]) Get(s string) *T {
	return this.root.get([]byte(s))
}

func (this *Trie[T]) Put(s string, val T) *T {
	return this.root.put([]byte(s), val)
}

func (this *Trie[T]) Remove(s string) *T {
	return this.root.delete([]byte(s))
}

func (this *Trie[T]) Count() int {
	return this.root.count.Get()
}

// return this to abort a Range without returning any error
var EOD = errors.New("EOD")

// Ranges over all the key/values in lexycographic order
func (this *Trie[T]) Range(f func(string, T) error) error {
	k := make([]byte, 0, 1024) // long enough?
	err := this.root.range_(k, func(k []byte, val T) error {
		return f(string(k), val)
	})
	if err == EOD {
		return nil
	}
	return err
}
