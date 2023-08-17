// should be posix safe so far otherwise //go:build linux || darwin

package goblin

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/ohait/goblin/trie"
	"golang.org/x/sys/unix"
)

type DB struct {
	m        sync.Mutex
	blocks   []byte
	dir      string
	dataname string
	data     *os.File
	logname  string
	log      *os.File

	t trie.Trie[[]int]

	pageSize int
	mmap     []byte
	free     []int // pages that can be used
	next     int   // next new page
	max      int   // max page

	Log func(f string, args ...any)
}

func (this *DB) String() string {
	return fmt.Sprintf("{%d keys, %d+%d free pages, %s data}",
		this.t.Count(), len(this.free), this.max-this.next, mb(this.max*this.pageSize))
}

// use the given directory as a DB.
// if missing, it's created
// if empty, a new DB is created
// NOTE: you can't use the same DB twice at the same time, not even on the same process
func New(dir string) (*DB, error) {
	var err error
	this := &DB{
		dir:      dir,
		pageSize: 256,
		Log:      func(f string, args ...any) {},
	}
	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return nil, fmt.Errorf("can't use %q: %w", dir, err)
	}

	this.dataname = filepath.Join(dir, "data.db")
	this.logname = filepath.Join(dir, "index.log")

	this.log, err = os.OpenFile(this.logname, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("can't open %q: %w", this.logname, err)
	}

	this.data, err = os.OpenFile(this.dataname, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("can't open %q: %w", this.dataname, err)
	}

	err = syscall.Flock(int(this.data.Fd()), syscall.LOCK_EX)
	if err != nil {
		return nil, fmt.Errorf("can't flock: %w", err)
	}

	s, _ := this.data.Stat()
	fsize := s.Size()
	if fsize == 0 {
		this.data.Truncate(1 << 20) // 1MB
		fsize = 1 << 20
	}
	this.mmap, err = syscall.Mmap(int(this.data.Fd()), 0, int(fsize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("can't mmap: %w", err)
	}
	this.max = int(fsize / int64(this.pageSize))
	//log.Printf("mmap at %p, max pages: %d", this.mmap, this.max)

	return this, this.rewind()
}

func (this *DB) Close() error {
	this.log.Close()
	syscall.Munmap(this.mmap)
	syscall.Flock(int(this.data.Fd()), syscall.LOCK_UN)
	return this.data.Close()
}

// Key/Value pair with lazy value retrieval
type Pair struct {
	Key   string
	sto   *DB
	size  int
	pages []int
}

func (this Pair) Fetch() []byte {
	return this.sto.fetch(this.size, this.pages)
}

// range thru all the keys in lexicographic order, and return each as a Pair
func (this *DB) Range(cb func(Pair) error) error {
	return this.t.Range(func(key string, record []int) error {
		size, pages := (record)[0], (record)[1:]
		return cb(Pair{key, this, size, pages})
	})
}

func (this *DB) Fetch(key string) ([]byte, error) {
	//log.Printf("fetch %q", key)
	record := this.t.Get(key)
	if record == nil {
		return nil, nil
	}
	size, pages := (*record)[0], (*record)[1:]
	//log.Printf("found key %q: size: %d, pages: %v", key, size, pages)
	return this.fetch(size, pages), nil
}

func (this *DB) Store(key string, data []byte) error {
	this.m.Lock()
	defer this.m.Unlock()

	record := []int{len(data)}
	for len(data) > 0 {
		var page int
		if len(this.free) > 0 {
			page, this.free = this.free[0], this.free[1:]
		} else {
			if this.next == this.max {
				this.grow()
			}
			page = this.next
			this.next++
		}
		record = append(record, page)
		start := page * this.pageSize
		end := start + this.pageSize
		//log.Printf("copy(mmap[%d:%d], data)", start, end)
		ct := copy(this.mmap[start:end], data)
		//log.Printf("stored %d bytes in page %d", ct, page)
		data = data[ct:]
	}

	_, err := this.log.WriteString(formatLog(key, record) + "\n")
	if err != nil {
		return fmt.Errorf("can't write log: %w", err)
	}

	old := this.t.Put(key, record)
	if old != nil {
		// put the old pages in the free list
		this.free = append(this.free, *old...)
	}
	return nil
}

func (this *DB) sync() error {
	err := unix.Msync(this.mmap, unix.MS_SYNC)
	if err != nil {
		return err
	}
	return this.log.Sync()
}
