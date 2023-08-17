package goblin

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// rebuil the log and drop old entries
func (this *DB) relog() error {
	this.m.Lock()
	defer this.m.Unlock()

	panic("NIY")
}

// on start, we replay the log to reconstruct the unused pages and the trie
func (this *DB) rewind() error {
	this.m.Lock()
	defer this.m.Unlock()

	_, err := this.log.Seek(0, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("can't seek: %w", err)
	}

	fs, _ := this.data.Stat()

	lenPages := (fs.Size() + int64(this.pageSize) - 1) / int64(this.pageSize)

	// we need a map of the used blocks, to build the free-blocks-list
	used := make([]uint64, (lenPages+63)/64)

	r := bufio.NewScanner(this.log)
	for r.Scan() {
		id, _, pages := parseLog(r.Text())
		//log.Printf("rewind %q in %v", id, pages)
		old := this.t.Put(id, pages)
		if old != nil {
			for _, page := range *old {
				used[page/64] &= ^(uint64(1) << (page % 64))
			}
		}
		for _, page := range pages {
			used[page/64] |= (1 << (uint64(page) % 64))
			if page >= this.next {
				this.next = page + 1
			}
		}
	}

	for page := 0; page < this.next; page++ {
		u := used[page/64] & (1 << (page % 64))
		//log.Printf("page %d is %d (%b)", page, u, used[page/64])
		if u == 0 {
			this.free = append(this.free, page)
		}
	}
	log.Printf("rewind done, %d free pages, next new page at %d", len(this.free), this.next)
	return nil
}

func parseLog(ln string) (key string, size int, pages []int) {
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
	return name, i[0], i[1:]
}

func formatLog(key string, pages []int) string {
	parts := []string{key}
	for _, page := range pages {
		parts = append(parts, fmt.Sprint(page))
	}
	return strings.Join(parts, " ")
}
