package goblin_test

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/ohait/goblin"
)

func TestConc(t *testing.T) {
	//goblin.Logger = t.Logf
	if testing.Short() {
		t.SkipNow()
	}

	_ = os.RemoveAll("/tmp/test-goblin")
	db, err := goblin.New("/tmp/test-goblin/")
	noError(t, err)
	t.Logf("init %+v", db)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			testConc(t, db)
		}()
	}
	wg.Wait()
}

func testConc(t *testing.T, db *goblin.DB) {
	data := make([]byte, 900)
	for i := 0; i < len(data); i++ {
		data[i] = byte('0' + (i % 26))
	}

	for i := 0; i < 1000; i++ {
		err := db.Store(fmt.Sprint(i), data)
		noError(t, err)
	}

	for i := 0; i < 1000; i++ {
		x, err := db.Fetch(fmt.Sprint(i))
		noError(t, err)
		if bytes.Compare(x, data) != 0 {
			t.Fatalf("expected\n%d %s got\n%d %s", len(data), string(data), len(x), string(x))
		}

		err = db.Store(fmt.Sprint(i), data)
		noError(t, err)
	}
}
