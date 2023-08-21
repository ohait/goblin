package goblin

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

func mb(size int) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	} else if size < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(size)/1024)
	} else if size < 1024*1024*1024 {
		return fmt.Sprintf("%.1fMB", float64(size)/1024/1024)
	} else {
		return fmt.Sprintf("%.1fGB", float64(size)/1024/1024/1024)
	}
}

func (this *DB) remmap(fsize int) error {
	var err error
	if this.mmap != nil {
		err = unix.Munmap(this.mmap)
		if err != nil {
			return fmt.Errorf("munmap: %w", err)
		}
	}
	this.mmap, err = unix.Mmap(int(this.data.Fd()), 0, fsize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED_VALIDATE)
	return err
}

func (this *DB) grow() error {
	t0 := time.Now()
	this.max *= 2
	newSize := this.max * this.pageSize
	err := this.data.Truncate(int64(newSize))
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	err = this.remmap(newSize)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	Logger("grow to %s in %v", mb(newSize), time.Since(t0))
	return nil
}

func (this *DB) fetch(size int, pages []int) []byte {
	out := make([]byte, 0, size)
	todo := size
	this.m.Lock()
	defer this.m.Unlock()
	for _, page := range pages {
		snap := todo
		if snap > this.pageSize {
			snap = this.pageSize
		}
		todo -= snap
		start := page * this.pageSize
		end := start + snap
		//Logger("fetch %d from page %d (%q)", snap, page, string(this.mmap[start:end]))
		out = append(out, this.mmap[start:end]...)
	}
	return out
}
