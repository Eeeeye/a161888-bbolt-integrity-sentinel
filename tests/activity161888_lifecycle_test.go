package bbolt

import (
	"bytes"
	stderrs "errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	bberrors "go.etcd.io/bbolt/errors"
)

func activity161888CatchPanic(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return nil
}

func activity161888AwaitTx(t *testing.T, done <-chan error, leaked *atomic.Pointer[Tx], label string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		if tx := leaked.Load(); tx != nil && tx.db != nil {
			tx.rollback()
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		t.Fatalf("%s remained blocked after a managed callback panic", label)
		return nil
	}
}

func TestActivity161888ManagedUpdatePanicRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "managed-update-panic.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("records"))
		return err
	}); err != nil {
		t.Fatalf("create update-panic fixture: %v", err)
	}

	sentinel := &struct{ name string }{"managed-update"}
	var leaked atomic.Pointer[Tx]
	recovered := activity161888CatchPanic(func() {
		_ = db.Update(func(tx *Tx) error {
			leaked.Store(tx)
			if err := tx.Bucket([]byte("records")).Put([]byte("uncommitted"), []byte("value")); err != nil {
				panic(err)
			}
			panic(sentinel)
		})
	})
	if recovered != sentinel {
		t.Fatalf("managed update recovered %#v, want original panic %#v", recovered, sentinel)
	}

	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(tx *Tx) error {
			return tx.Bucket([]byte("records")).Put([]byte("committed"), []byte("ok"))
		})
	}()
	if err := activity161888AwaitTx(t, done, &leaked, "subsequent write"); err != nil {
		t.Fatalf("write after managed update panic: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("records"))
		if got := b.Get([]byte("uncommitted")); got != nil {
			return fmt.Errorf("panicking update committed value %q", got)
		}
		if got := b.Get([]byte("committed")); !bytes.Equal(got, []byte("ok")) {
			return fmt.Errorf("post-panic write value=%q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify managed update panic: %v", err)
	}
}

func TestActivity161888ManagedViewPanicReleasesSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "managed-view-panic.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	if err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("records"))
		if err != nil {
			return err
		}
		return b.Put([]byte("anchor"), []byte("value"))
	}); err != nil {
		t.Fatalf("create view-panic fixture: %v", err)
	}

	sentinel := &struct{ name string }{"managed-view"}
	var leaked atomic.Pointer[Tx]
	recovered := activity161888CatchPanic(func() {
		_ = db.View(func(tx *Tx) error {
			leaked.Store(tx)
			if got := tx.Bucket([]byte("records")).Get([]byte("anchor")); !bytes.Equal(got, []byte("value")) {
				panic(fmt.Sprintf("anchor=%q", got))
			}
			panic(sentinel)
		})
	})
	if recovered != sentinel {
		t.Fatalf("managed view recovered %#v, want original panic %#v", recovered, sentinel)
	}

	done := make(chan error, 1)
	go func() {
		done <- db.View(func(tx *Tx) error {
			if got := tx.Bucket([]byte("records")).Get([]byte("anchor")); !bytes.Equal(got, []byte("value")) {
				return fmt.Errorf("anchor=%q", got)
			}
			return nil
		})
	}()
	if err := activity161888AwaitTx(t, done, &leaked, "subsequent read"); err != nil {
		t.Fatalf("read after managed view panic: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close after managed view panic: %v", err)
		}
	case <-time.After(2 * time.Second):
		if tx := leaked.Load(); tx != nil && tx.db != nil {
			tx.rollback()
		}
		select {
		case <-closeDone:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("managed view panic leaked a snapshot that blocked Close")
	}
}

func TestActivity161888BatchPanicDoesNotLeakSoloRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch-panic.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	db.MaxBatchSize = 1
	db.MaxBatchDelay = time.Millisecond
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("records"))
		return err
	}); err != nil {
		t.Fatalf("create batch-panic fixture: %v", err)
	}

	sentinel := &struct{ name string }{"batch"}
	var leaked atomic.Pointer[Tx]
	recovered := activity161888CatchPanic(func() {
		_ = db.Batch(func(tx *Tx) error {
			leaked.Store(tx)
			panic(sentinel)
		})
	})
	if recovered != sentinel {
		t.Fatalf("batch recovered %#v, want original panic %#v", recovered, sentinel)
	}

	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(tx *Tx) error {
			return tx.Bucket([]byte("records")).Put([]byte("after-batch-panic"), []byte("ok"))
		})
	}()
	if err := activity161888AwaitTx(t, done, &leaked, "write after batched panic"); err != nil {
		t.Fatalf("write after batched panic: %v", err)
	}
}

func TestActivity161888CloseWaitsForReadSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close-reader.db")
	db, err := Open(path, 0o600, &Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open close-reader fixture: %v", err)
	}
	if err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("records"))
		if err != nil {
			return err
		}
		return b.Put([]byte("anchor"), []byte("value"))
	}); err != nil {
		_ = db.Close()
		t.Fatalf("populate close-reader fixture: %v", err)
	}

	rtx, err := db.Begin(false)
	if err != nil {
		_ = db.Close()
		t.Fatalf("begin read snapshot: %v", err)
	}
	started := make(chan struct{})
	closed := make(chan error, 1)
	go func() {
		close(started)
		closed <- db.Close()
	}()
	<-started

	select {
	case closeErr := <-closed:
		_ = rtx.Rollback()
		t.Fatalf("Close returned while read snapshot was open: %v", closeErr)
	case <-time.After(200 * time.Millisecond):
	}
	if got := rtx.Bucket([]byte("records")).Get([]byte("anchor")); !bytes.Equal(got, []byte("value")) {
		_ = rtx.Rollback()
		t.Fatalf("reader snapshot became unusable while Close waited: %q", got)
	}
	if err := rtx.Rollback(); err != nil {
		t.Fatalf("finish read snapshot: %v", err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after reader completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not finish after read snapshot ended")
	}
	if tx, err := db.Begin(false); tx != nil || !stderrs.Is(err, bberrors.ErrDatabaseNotOpen) {
		if tx != nil {
			_ = tx.Rollback()
		}
		t.Fatalf("Begin after Close returned tx=%v err=%v", tx, err)
	}

	reopened, err := Open(path, 0o600, &Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("reopen after Close released file lock: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened database: %v", err)
	}
}

func TestActivity161888CloseWaitsForWriteTransaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close-writer.db")
	db, err := Open(path, 0o600, &Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open close-writer fixture: %v", err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("records"))
		return err
	}); err != nil {
		_ = db.Close()
		t.Fatalf("populate close-writer fixture: %v", err)
	}

	wtx, err := db.Begin(true)
	if err != nil {
		_ = db.Close()
		t.Fatalf("begin write transaction: %v", err)
	}
	if err := wtx.Bucket([]byte("records")).Put([]byte("before-close"), []byte("pending")); err != nil {
		_ = wtx.Rollback()
		_ = db.Close()
		t.Fatalf("write before concurrent Close: %v", err)
	}

	started := make(chan struct{})
	closed := make(chan error, 1)
	go func() {
		close(started)
		closed <- db.Close()
	}()
	<-started

	select {
	case closeErr := <-closed:
		_ = wtx.Rollback()
		t.Fatalf("Close returned while write transaction was open: %v", closeErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := wtx.Bucket([]byte("records")).Put([]byte("while-close-waits"), []byte("committed")); err != nil {
		_ = wtx.Rollback()
		t.Fatalf("write transaction became unusable while Close waited: %v", err)
	}
	if err := wtx.Commit(); err != nil {
		t.Fatalf("commit write transaction while Close waited: %v", err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after writer completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not finish after write transaction committed")
	}
	if tx, err := db.Begin(true); tx != nil || !stderrs.Is(err, bberrors.ErrDatabaseNotOpen) {
		if tx != nil {
			_ = tx.Rollback()
		}
		t.Fatalf("writable Begin after Close returned tx=%v err=%v", tx, err)
	}

	reopened, err := Open(path, 0o600, &Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("reopen after writer and Close released file lock: %v", err)
	}
	if err := reopened.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("records"))
		if got := b.Get([]byte("before-close")); !bytes.Equal(got, []byte("pending")) {
			return fmt.Errorf("first committed writer value=%q", got)
		}
		if got := b.Get([]byte("while-close-waits")); !bytes.Equal(got, []byte("committed")) {
			return fmt.Errorf("second committed writer value=%q", got)
		}
		return nil
	}); err != nil {
		_ = reopened.Close()
		t.Fatalf("verify committed writer after concurrent Close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened writer database: %v", err)
	}
}

func TestActivity161888MoveBucketRejectsCrossDatabase(t *testing.T) {
	srcPath := filepath.Join(t.TempDir(), "move-src.db")
	dstPath := filepath.Join(t.TempDir(), "move-dst.db")
	src := activity161888Open(t, srcPath, &Options{PageSize: 4096})
	dst := activity161888Open(t, dstPath, &Options{PageSize: 4096})
	if err := src.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("source"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucket([]byte("moving"))
		if err != nil {
			return err
		}
		return child.Put([]byte("anchor"), []byte("source-value"))
	}); err != nil {
		t.Fatalf("populate source database: %v", err)
	}
	if err := dst.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("target"))
		return err
	}); err != nil {
		t.Fatalf("populate target database: %v", err)
	}

	srcTx, err := src.Begin(true)
	if err != nil {
		t.Fatalf("begin source write transaction: %v", err)
	}
	dstTx, err := dst.Begin(true)
	if err != nil {
		_ = srcTx.Rollback()
		t.Fatalf("begin target write transaction: %v", err)
	}
	moveErr := srcTx.Bucket([]byte("source")).MoveBucket([]byte("moving"), dstTx.Bucket([]byte("target")))
	if !stderrs.Is(moveErr, bberrors.ErrDifferentDB) {
		_ = srcTx.Rollback()
		_ = dstTx.Rollback()
		t.Fatalf("cross-database MoveBucket returned %v, want %v", moveErr, bberrors.ErrDifferentDB)
	}
	if err := srcTx.Rollback(); err != nil {
		_ = dstTx.Rollback()
		t.Fatalf("rollback source after rejected move: %v", err)
	}
	if err := dstTx.Rollback(); err != nil {
		t.Fatalf("rollback target after rejected move: %v", err)
	}

	if err := src.View(func(tx *Tx) error {
		moving := tx.Bucket([]byte("source")).Bucket([]byte("moving"))
		if moving == nil || !bytes.Equal(moving.Get([]byte("anchor")), []byte("source-value")) {
			return fmt.Errorf("source bucket changed after rejected move")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify source after rejected move: %v", err)
	}
	if err := dst.View(func(tx *Tx) error {
		if tx.Bucket([]byte("target")).Bucket([]byte("moving")) != nil {
			return fmt.Errorf("target received bucket from rejected move")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify target after rejected move: %v", err)
	}
}
