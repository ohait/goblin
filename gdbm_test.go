package goblin_test

/*
func TestGDBM(t *testing.T) {
	_ = os.RemoveAll("/tmp/test-mmap")
	os.MkdirAll("/tmp/test-mmap", 0777)
	db, err := gdbm.Open("/tmp/test-mmap/gdbm.db", "c")
	noError(t, err)
	t.Logf("init %+v", db)

	wct := 0
	t.Run("write", func(t *testing.T) {
		t0 := time.Now()
		tot := 0
		for i := 0; time.Since(t0) < 10*time.Second; i++ {
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
			db.Replace(key, string(x))
		}
		dt := time.Since(t0)
		t.Logf("wrote %d entries, %.2f MB, in %v", wct, float64(tot)/1024/1024, dt)
	})
	t.Run("read", func(t *testing.T) {
		t0 := time.Now()
		tot := 0
		ct := 0
		for i := 0; time.Since(t0) < 10*time.Second; i = (i + 1) % wct {
			key := fmt.Sprintf("prefix-%08x", i)
			ct++
			data, _ := db.Fetch(key)
			tot += len(data)
		}
		dt := time.Since(t0)
		t.Logf("read %d entries, %.2f MB, in %v", ct, float64(tot)/1024/1024, dt)
	})
}
*/
