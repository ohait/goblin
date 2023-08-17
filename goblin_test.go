package goblin_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ohait/goblin"
)

func noError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Logf("no error")
	} else {
		t.Fatal(err)
	}
}

func TestDB(t *testing.T) {
	_ = os.RemoveAll("/tmp/test-goblin")
	db, err := goblin.New("/tmp/test-goblin/")
	noError(t, err)
	t.Logf("init %+v", db)

	x, err := db.Fetch("oha")
	noError(t, err)
	if len(x) > 0 {
		t.Fatalf("expected no data")
	}

	noError(t, db.Store("oha", []byte("Miss")))
	noError(t, db.Store("oha", []byte("Oha")))

	x, err = db.Fetch("oha")
	noError(t, err)
	if string(x) != "Oha" {
		t.Fatalf("expected Oha, got %q", x)
	}

	fs, err := os.Stat("/tmp/test-goblin/index.log")
	noError(t, err)
	size0 := fs.Size()

	err = db.Optimize()
	noError(t, err)

	fs, err = os.Stat("/tmp/test-goblin/index.log")
	noError(t, err)
	size1 := fs.Size()
	if size1 >= size0 {
		t.Fatalf("Optimize: %d -> %d", size0, size1)
	}
	t.Logf("Optimize: %d -> %d", size0, size1)

	noError(t, db.Close())

	db, err = goblin.New("/tmp/test-goblin/")
	noError(t, err)
	db.Log = t.Logf
	t.Logf("reopened: %d", db.Size())
	_ = db.Range(func(p goblin.Pair) error {
		t.Logf("%q => %s", p.Key, p.Fetch())
		return nil
	})

	x, err = db.Fetch("oha")
	noError(t, err)
	db.Log = t.Logf
	if string(x) != "Oha" {
		t.Fatalf("expected Oha, got %q", x)
	}

	db.Close()
}

func TestScale(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	_ = os.RemoveAll("/tmp/test-goblin")
	db, err := goblin.New("/tmp/test-goblin/")
	noError(t, err)
	//db.Log = t.Logf
	t.Logf("init %+v", db)

	wct := 0
	t.Run("write", func(t *testing.T) {
		t0 := time.Now()
		tot := 0
		for i := 0; time.Since(t0) < time.Second; i++ {
			key := fmt.Sprintf("prefix-%08x", i)
			data := key
			for x := 0; x < i%14; x++ {
				data = data + data
			}
			x := []byte(data)
			lx := len(x)
			tot += lx
			wct++
			//t.Logf("key: %s, data %d bytes", key, lx)
			_ = db.Store(key, x)
		}
		dt := time.Since(t0)
		t.Logf("wrote %d entries, %.2f MB, in %v", wct, float64(tot)/1024/1024, dt)
	})
	t.Run("read", func(t *testing.T) {
		t0 := time.Now()
		tot := 0
		ct := 0
		for i := 0; time.Since(t0) < time.Second; i = (i + 1) % wct {
			key := fmt.Sprintf("prefix-%08x", i)
			ct++
			data, _ := db.Fetch(key)
			tot += len(data)
		}
		dt := time.Since(t0)
		t.Logf("read %d entries, %.2f MB, in %v", ct, float64(tot)/1024/1024, dt)
	})
	t.Run("range-no-fetch", func(t *testing.T) {
		ct := 0
		t0 := time.Now()
		_ = db.Range(func(p goblin.Pair) error {
			ct++
			return nil
		})
		dt := time.Since(t0)
		t.Logf("read %d entries in %v (%.2fK per sec)", ct, dt, float64(ct)/1000/dt.Seconds())
	})
	t.Run("range-and-fetch", func(t *testing.T) {
		ct := 0
		tot := 0
		t0 := time.Now()
		_ = db.Range(func(p goblin.Pair) error {
			data := p.Fetch()
			tot += len(data)
			ct++
			return nil
		})
		dt := time.Since(t0)
		t.Logf("read %d entries for %.2e bytes in %v (%.2fK per sec)", ct, float64(tot), dt, float64(ct)/1000/dt.Seconds())
	})
}
