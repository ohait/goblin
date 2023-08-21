package goblin

import (
	"bufio"
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
		_, err := newlog.WriteString(formatLog(k, r) + "\n")
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
		id, record := parseLog(r.Text())
		ct++
		//log.Printf("rewind %q in %v: %q", id, record, r.Text())
		old := this.trie.Put(id, record)
		if old != nil {
			for _, page := range *old {
				used[page/64] &= ^(uint64(1) << (page % 64))
			}
		}
		for _, page := range record[1:] {
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

func parseLog(ln string) (key string, record []int) {
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
	return name, i
}

func formatLog(key string, record []int) string {
	parts := []string{key}
	for _, page := range record {
		parts = append(parts, fmt.Sprint(page))
	}
	return strings.Join(parts, " ")
}