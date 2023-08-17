package trie_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/ohait/goblin/trie"
)

func assert(t *testing.T, v bool, f string, args ...any) {
	t.Helper()
	if v {
		t.Logf(f, args...)
	} else {
		t.Fatalf(f, args...)
	}
}
func equal(t *testing.T, expect, got any) {
	t.Helper()
	e := fmt.Sprint(expect)
	g := fmt.Sprint(got)
	if e == g {
		t.Logf("%s", e)
	} else {
		t.Fatalf("expected %q, got %q", e, g)
	}
}

func TestShow(t *testing.T) {
	var x trie.Trie[int]
	x.Put("prefix-001", 1)
	x.Put("prefix-002", 2)
	x.Put("prefix-042", 42)
	x.Put("prefix-042", 42)
	t.Logf("%v", &x)
}

func TestTrie(t *testing.T) {
	var x trie.Trie[string]
	list := func() (out []string) {
		x.Range(func(k string, val string) error {
			out = append(out, k+" "+val)
			return nil
		})
		return out
	}
	equal(t, 0, len(list()))
	equal(t, nil, x.Put("a", "A"))
	assert(t, x.Put("a", "A") != nil, "a -> !nil")
	equal(t, 1, len(list()))
	assert(t, x.Put("a2", "2") == nil, "a2 -> nil")
	assert(t, x.Put("a1", "1") == nil, "a1 -> nil")
	equal(t, "a A,a1 1,a2 2", strings.Join(list(), ","))

	assert(t, x.Remove("a1") != nil, "remove a1")
	equal(t, "a A,a2 2", strings.Join(list(), ","))
}

func TestMem(t *testing.T) {
	SIZE := 100000
	var x trie.Trie[int]
	m := map[string]int{}
	for i := 0; i < SIZE; i++ {
		m[fmt.Sprintf("loooooong-prefix-%08x", i)] = i
	}
	x.Put("init", -1)
	for k := range m {
		t.Logf("key length: %d (%q)", len(k), k)
		break
	}

	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	for k, v := range m {
		x.Put(k, v)
	}
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	t.Logf("mem: %f avg bytes per entry", float64(m1.Alloc-m0.Alloc)/float64(SIZE))
}
