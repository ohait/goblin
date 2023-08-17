package goblin

import (
	"fmt"
	"syscall"
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

func (this *DB) grow() error {
	t0 := time.Now()
	this.max *= 2
	newSize := this.max * this.pageSize
	err := this.data.Truncate(int64(newSize))
	if err != nil {
		return err
	}
	syscall.Munmap(this.mmap)
	this.mmap, err = unix.Mmap(int(this.data.Fd()), 0, newSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED_VALIDATE)
	if err != nil {
		return err
	}
	this.Log("grow to %s in %v", mb(newSize), time.Since(t0))
	return nil
}

func (this *DB) fetch(size int, pages []int) []byte {
	out := make([]byte, 0, size)
	this.m.Lock()
	defer this.m.Unlock()
	for _, page := range pages {
		todo := cap(out)
		if todo > this.pageSize {
			todo = this.pageSize
		}
		start := page * this.pageSize
		end := start + todo
		out = append(out, this.mmap[start:end]...)
	}
	return out
}
