package bbolt

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/bbolt/internal/guts_cli"
)

func TestActivity161888CursorRecoversAfterReverseExhaustion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor-direction.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	activity161888Fill(t, db, []byte("records"), 1000, 32)

	err := db.View(func(tx *Tx) error {
		c := tx.Bucket([]byte("records")).Cursor()
		if key, _ := c.Seek([]byte("00000002")); !bytes.Equal(key, []byte("00000002")) {
			return fmt.Errorf("seek returned %q", key)
		}
		for _, want := range [][]byte{[]byte("00000001"), []byte("00000000")} {
			if key, _ := c.Prev(); !bytes.Equal(key, want) {
				return fmt.Errorf("reverse iteration returned %q, want %q", key, want)
			}
		}
		for i := 0; i < 3; i++ {
			if key, _ := c.Prev(); key != nil {
				return fmt.Errorf("Prev at beginning returned %q on call %d", key, i+1)
			}
		}
		if key, _ := c.Next(); !bytes.Equal(key, []byte("00000001")) {
			return fmt.Errorf("Next after reverse exhaustion returned %q, want %q", key, []byte("00000001"))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cursor direction recovery: %v", err)
	}
}

func TestActivity161888CursorLastPreservesEmptyBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor-empty.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("empty"))
		return err
	}); err != nil {
		t.Fatalf("create empty bucket: %v", err)
	}
	if err := db.View(func(tx *Tx) error {
		c := tx.Bucket([]byte("empty")).Cursor()
		if key, value := c.Last(); key != nil || value != nil {
			return fmt.Errorf("Last on empty bucket returned %q/%q", key, value)
		}
		if key, value := c.Next(); key != nil || value != nil {
			return fmt.Errorf("Next after empty Last returned %q/%q", key, value)
		}
		return nil
	}); err != nil {
		t.Fatalf("empty cursor control: %v", err)
	}
}

func TestActivity161888CopyFileUsesTransactionFileSnapshot(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("atomic path replacement fixture requires Linux rename semantics")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "primary.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	activity161888Fill(t, db, []byte("records"), 1200, 256)
	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin snapshot: %v", err)
	}

	replacementPath := filepath.Join(dir, "replacement.db")
	replacement, err := Open(replacementPath, 0o600, &Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	if err := replacement.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("replacement"))
		if err != nil {
			return err
		}
		return b.Put([]byte("different"), []byte("inode"))
	}); err != nil {
		t.Fatalf("populate replacement: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatalf("close replacement: %v", err)
	}
	if err := os.Rename(replacementPath, path); err != nil {
		t.Fatalf("replace database path: %v", err)
	}

	backupPath := filepath.Join(dir, "snapshot.db")
	if err := rtx.CopyFile(backupPath, 0o600); err != nil {
		_ = rtx.Rollback()
		t.Fatalf("copy transaction after path replacement: %v", err)
	}
	if err := rtx.Rollback(); err != nil {
		t.Fatalf("close source snapshot: %v", err)
	}

	backup, err := Open(backupPath, 0o600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open copied snapshot: %v", err)
	}
	defer backup.Close()
	if err := backup.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("records"))
		if b == nil {
			return fmt.Errorf("copied snapshot lost original bucket")
		}
		if got := b.Get([]byte("00001199")); len(got) != 256 {
			return fmt.Errorf("copied snapshot value length=%d, want 256", len(got))
		}
		if tx.Bucket([]byte("replacement")) != nil {
			return fmt.Errorf("copied snapshot used replacement inode")
		}
		for checkErr := range tx.Check() {
			return checkErr
		}
		return nil
	}); err != nil {
		t.Fatalf("validate copied snapshot: %v", err)
	}
}

func TestActivity161888WriteToWithNonzeroWriteFlagUsesTransactionFileSnapshot(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("atomic path replacement fixture requires Linux rename semantics")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "write-flag-primary.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	activity161888Fill(t, db, []byte("records"), 900, 256)
	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin flagged snapshot: %v", err)
	}
	rtx.WriteFlag = syscall.O_CLOEXEC
	wantSize := rtx.Size()

	replacementPath := filepath.Join(dir, "write-flag-replacement.db")
	replacement, err := Open(replacementPath, 0o600, &Options{PageSize: 4096})
	if err != nil {
		_ = rtx.Rollback()
		t.Fatalf("open flagged replacement: %v", err)
	}
	if err := replacement.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("replacement"))
		if err != nil {
			return err
		}
		return b.Put([]byte("different"), []byte("inode"))
	}); err != nil {
		_ = rtx.Rollback()
		t.Fatalf("populate flagged replacement: %v", err)
	}
	if err := replacement.Close(); err != nil {
		_ = rtx.Rollback()
		t.Fatalf("close flagged replacement: %v", err)
	}
	if err := os.Rename(replacementPath, path); err != nil {
		_ = rtx.Rollback()
		t.Fatalf("replace flagged database path: %v", err)
	}

	var snapshot bytes.Buffer
	written, writeErr := rtx.WriteTo(&snapshot)
	rollbackErr := rtx.Rollback()
	if writeErr != nil {
		t.Fatalf("WriteTo with nonzero WriteFlag after path replacement: %v", writeErr)
	}
	if rollbackErr != nil {
		t.Fatalf("close flagged source snapshot: %v", rollbackErr)
	}
	if written != wantSize || int64(snapshot.Len()) != wantSize {
		t.Fatalf("flagged snapshot size written=%d buffer=%d want=%d", written, snapshot.Len(), wantSize)
	}

	backupPath := filepath.Join(dir, "write-flag-snapshot.db")
	if err := os.WriteFile(backupPath, snapshot.Bytes(), 0o600); err != nil {
		t.Fatalf("persist flagged snapshot: %v", err)
	}
	backup, err := Open(backupPath, 0o600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open flagged snapshot: %v", err)
	}
	defer backup.Close()
	if err := backup.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("records"))
		if b == nil {
			return fmt.Errorf("flagged snapshot lost original bucket")
		}
		if got := b.Get([]byte("00000899")); len(got) != 256 {
			return fmt.Errorf("flagged snapshot value length=%d, want 256", len(got))
		}
		if tx.Bucket([]byte("replacement")) != nil {
			return fmt.Errorf("nonzero WriteFlag switched to replacement inode")
		}
		for checkErr := range tx.Check() {
			return checkErr
		}
		return nil
	}); err != nil {
		t.Fatalf("validate flagged snapshot: %v", err)
	}
}

type activity161888SlowWriter struct {
	file *os.File
}

func (w activity161888SlowWriter) Write(p []byte) (int, error) {
	time.Sleep(250 * time.Microsecond)
	return w.file.Write(p)
}

func TestActivity161888ConcurrentWriteToUsesIndependentOffsets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent-copy.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	activity161888Fill(t, db, []byte("records"), 5000, 512)

	const copies = 4
	errs := make(chan error, copies)
	var wg sync.WaitGroup
	for i := 0; i < copies; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			tx, err := db.Begin(false)
			if err != nil {
				errs <- fmt.Errorf("copy %d begin: %w", index, err)
				return
			}
			defer tx.Rollback()
			copyPath := filepath.Join(dir, fmt.Sprintf("copy-%d.db", index))
			f, err := os.OpenFile(copyPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
			if err != nil {
				errs <- fmt.Errorf("copy %d open: %w", index, err)
				return
			}
			_, writeErr := tx.WriteTo(activity161888SlowWriter{file: f})
			closeErr := f.Close()
			if writeErr != nil {
				errs <- fmt.Errorf("copy %d write: %w", index, writeErr)
				return
			}
			if closeErr != nil {
				errs <- fmt.Errorf("copy %d close: %w", index, closeErr)
				return
			}
			copyDB, err := Open(copyPath, 0o600, &Options{ReadOnly: true})
			if err != nil {
				errs <- fmt.Errorf("copy %d reopen: %w", index, err)
				return
			}
			defer copyDB.Close()
			err = copyDB.View(func(checkTx *Tx) error {
				b := checkTx.Bucket([]byte("records"))
				if b == nil || len(b.Get([]byte("00004999"))) != 512 {
					return fmt.Errorf("last record missing")
				}
				for checkErr := range checkTx.Check() {
					return checkErr
				}
				return nil
			})
			if err != nil {
				errs <- fmt.Errorf("copy %d validate: %w", index, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestActivity161888NoFreelistSyncGrowsBeforePageWrites(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file growth semantics differ on Windows")
	}

	for _, noFreelistSync := range []bool{false, true} {
		t.Run(fmt.Sprintf("no-freelist-sync=%t", noFreelistSync), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "growth.db")
			db := activity161888Open(t, path, &Options{PageSize: 4096, NoFreelistSync: noFreelistSync})
			largeValue := make([]byte, db.AllocSize/100)
			for count := 0; count < 200; count++ {
				err := db.Update(func(tx *Tx) error {
					b, err := tx.CreateBucketIfNotExists([]byte("records"))
					if err != nil {
						return err
					}
					return b.Put([]byte(fmt.Sprintf("%04d", count)), largeValue)
				})
				if err != nil {
					t.Fatalf("commit %d: %v", count, err)
				}
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("stat commit %d: %v", count, err)
				}
				size := info.Size()
				if size > int64(db.AllocSize) && size < int64(db.AllocSize)*2 {
					t.Fatalf("file grew piecemeal across allocation boundary: size=%d alloc=%d", size, db.AllocSize)
				}
				if size > int64(db.AllocSize) {
					return
				}
			}
			t.Fatal("fixture did not cross the allocation boundary")
		})
	}
}

func TestActivity161888OpenFallsBackFromCorruptNewestMeta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta-fallback.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	if err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("records"))
		if err != nil {
			return err
		}
		return b.Put([]byte("anchor"), []byte("committed"))
	}); err != nil {
		t.Fatalf("write fallback transaction: %v", err)
	}
	if err := db.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("records")).Put([]byte("newest-only"), []byte("discarded"))
	}); err != nil {
		t.Fatalf("write newest transaction: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before meta corruption: %v", err)
	}

	active, activeID, err := guts_cli.GetActiveMetaPage(path)
	if err != nil {
		t.Fatalf("locate active meta: %v", err)
	}
	activeTxid := active.Txid()
	p, buf, err := guts_cli.ReadPage(path, uint64(activeID))
	if err != nil {
		t.Fatalf("read active meta page: %v", err)
	}
	p.Meta().SetChecksum(p.Meta().Checksum() ^ 1)
	if err := guts_cli.WritePage(path, buf); err != nil {
		t.Fatalf("corrupt active meta checksum: %v", err)
	}

	recovered, err := Open(path, 0o600, &Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open with one valid meta page: %v", err)
	}
	defer recovered.Close()
	if err := recovered.View(func(tx *Tx) error {
		if tx.ID() >= int(activeTxid) {
			return fmt.Errorf("used corrupt newest meta txid %d (active %d)", tx.ID(), activeTxid)
		}
		b := tx.Bucket([]byte("records"))
		if b == nil || !bytes.Equal(b.Get([]byte("anchor")), []byte("committed")) {
			return fmt.Errorf("fallback transaction lost committed anchor")
		}
		if got := b.Get([]byte("newest-only")); got != nil {
			return fmt.Errorf("fallback exposed newest-only value %q", got)
		}
		for checkErr := range tx.Check() {
			return checkErr
		}
		return nil
	}); err != nil {
		t.Fatalf("validate fallback snapshot: %v", err)
	}
}
