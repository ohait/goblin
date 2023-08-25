package goblin

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// rebuild the log and drop old entries
// will aquire a global lock until done
func (this *DB) Optimize() error {
	this.m.Lock()
	defer this.m.Unlock()
	return this.optimize()
}

func (this *DB) optimize() error {
	Logger("Optimize")

	newlog, err := os.OpenFile(this.logname+"~", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	t := time.Now().Add(time.Second)
	err = this.trie.Range(func(k string, r []int) error {
		now := time.Now()
		if now.After(t) {
			t = now.Add(time.Second)
			Logger("Optimize %q...", k)
		}
		_, err := newlog.WriteString(record{k, r[0], r[1:]}.formatLog() + "\n")
		return err
	})
	if err != nil {
		return err
	}
	err = newlog.Sync()
	if err != nil {
		return err
	}
	err = os.Rename(this.logname+"~", this.logname)
	if err != nil {
		return err
	}
	this.log = newlog
	return nil
}

// on start, we replay the log to reconstruct the unused pages and the trie
func (this *DB) rewind() error {
	this.m.Lock()
	defer this.m.Unlock()

	_, err := this.log.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("can't seek: %w", err)
	}

	fs, _ := this.data.Stat()

	lenPages := (fs.Size() + int64(this.pageSize) - 1) / int64(this.pageSize)

	// we need a map of the used blocks, to build the free-blocks-list
	used := make([]uint64, (lenPages+63)/64)

	r := bufio.NewScanner(this.log)
	ct := 0
	for r.Scan() {
		record := parseLog(r.Text())
		ct++
		//log.Printf("rewind %q in %v: %q", id, record, r.Text())
		old := this.trie.Put(record.Key, record.val())
		if old != nil {
			for _, page := range *old {
				used[page/64] &= ^(uint64(1) << (page % 64))
			}
		}
		for _, page := range record.Pages {
			used[page/64] |= (1 << (uint64(page) % 64))
			if page >= this.next {
				this.next = page + 1
			}
		}
	}

	for page := 0; page < this.next; page++ {
		u := used[page/64] & (1 << (page % 64))
		//log.Printf("page %d use %v (%b)", page, u != 0, used[page/64])
		if u == 0 {
			this.unused = append(this.unused, page)
		}
	}

	if ct > this.trie.Count()*3/2+10 {
		err = this.optimize()
		if err != nil {
			return fmt.Errorf("can't rebuild log file: %w", err)
		}
	}

	Logger("rewind done, %d free pages, next new page at %d", len(this.unused), this.next)
	return nil
}

type record struct {
	Key   string `json:"key"`
	Size  int    `json:"size"`
	Pages []int  `json:"pages"`
}

func (this record) val() []int {
	return append([]int{this.Size}, this.Pages...)
}

func parseLog(ln string) (r record) {
	err := json.Unmarshal([]byte(ln), &r)
	if err != nil {
		return parseLog_(ln) // backward compat (only temporary)
		//panic(err)
	}
	return
}

func (this record) formatLog() string {
	j, err := json.Marshal(this)
	if err != nil {
		panic(err)
	}
	return string(j)
}

func parseLog_(ln string) record {
	parts := strings.Split(ln, " ")
	name, parts := parts[0], parts[1:]
	i := []int{}
	for _, part := range parts {
		id, err := strconv.Atoi(part)
		if err != nil {
			panic(err)
		}
		i = append(i, id)
	}
	return record{
		Key:   name,
		Size:  i[0],
		Pages: i[1:],
	}
}
