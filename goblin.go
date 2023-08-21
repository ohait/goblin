// should be posix safe so far otherwise //go:build linux || darwin

package goblin

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/ohait/goblin/trie"
	"golang.org/x/sys/unix"
)

var Logger = func(f string, args ...any) {}

type DB struct {
	m        sync.Mutex
	dir      string
	dataname string
	data     *os.File
	logname  string
	log      *os.File

	trie trie.Trie[[]int]

	pageSize int
	mmap     []byte
	unused   []int // pages that can be used
	next     int   // next new page
	max      int   // max page
}

func (this *DB) String() string {
	return fmt.Sprintf("{%d keys, %d+%d free pages, %s data}",
		this.trie.Count(), len(this.unused), this.max-this.next, mb(this.max*this.pageSize))
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

	err = unix.Flock(int(this.data.Fd()), unix.LOCK_EX)
	if err != nil {
		return nil, fmt.Errorf("can't flock: %w", err)
	}

	s, _ := this.data.Stat()
	fsize := s.Size()
	if fsize == 0 {
		err = this.data.Truncate(1 << 20) // 1MB
		if err != nil {
			return nil, fmt.Errorf("can't create data file: %w", err)
		}
		fsize = 1 << 20
	}
	err = this.remmap(int(fsize))
	if err != nil {
		return nil, fmt.Errorf("can't mmap: %w", err)
	}
	this.max = int(fsize / int64(this.pageSize))
	//log.Printf("mmap at %p, max pages: %d", this.mmap, this.max)

	err = this.rewind()
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(this, func(obj any) {
		obj.(*DB).Close()
	})

	return this, nil
}

func (this *DB) Close() error {
	defer runtime.SetFinalizer(this, nil)
	_ = this.Sync()
	_ = this.log.Close()
	_ = unix.Munmap(this.mmap)
	_ = unix.Flock(int(this.data.Fd()), unix.LOCK_UN)
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
	return this.trie.Range(func(key string, record []int) error {
		size, pages := (record)[0], (record)[1:]
		return cb(Pair{key, this, size, pages})
	})
}

func (this *DB) Size() int {
	return this.trie.Count()
}

func (this *DB) Fetch(key string) ([]byte, error) {
	//Logger("fetch %q", key)
	record := this.trie.Get(key)
	if record == nil {
		Logger("not found %q", key)
		return nil, nil
	}
	size, pages := (*record)[0], (*record)[1:]
	Logger("found key %q: size: %d, pages: %v", key, size, pages)

	return this.fetch(size, pages), nil
}

func (this *DB) Store(key string, data []byte) error {
	this.m.Lock()
	defer this.m.Unlock()

	record := []int{len(data)}
	for len(data) > 0 {
		var page int
		if len(this.unused) > 0 {
			page, this.unused = this.unused[0], this.unused[1:]
			//Logger("using unused: %d", page)
		} else {
			//Logger("using next: %d, max: %d", this.next, this.max)
			if this.next == this.max {
				err := this.grow()
				if err != nil {
					log.Printf("grow error: %v", err)
					return err
				}
			}
			page = this.next
			this.next++
		}
		record = append(record, page)
		start := page * this.pageSize
		end := start + this.pageSize
		ct := copy(this.mmap[start:end], data)
		//Logger("stored %d in page %d (%q)", end-start, page, string(this.mmap[start:end]))
		data = data[ct:]
	}

	_, err := this.log.WriteString(formatLog(key, record) + "\n")
	if err != nil {
		return fmt.Errorf("can't write log: %w", err)
	}

	old := this.trie.Put(key, record)
	if old != nil {
		// put the old pages in the free list
		//Logger("now unused: %v", (*old)[1:])
		this.unused = append(this.unused, (*old)[1:]...)
	}
	return nil
}

func (this *DB) Sync() error {
	err := unix.Msync(this.mmap, unix.MS_SYNC)
	if err != nil {
		return err
	}
	return this.log.Sync()
}
