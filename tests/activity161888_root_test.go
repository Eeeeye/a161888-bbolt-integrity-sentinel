package bbolt

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func activity161888Open(t *testing.T, path string, options *Options) *DB {
	t.Helper()
	db, err := Open(path, 0o600, options)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() {
		if db.opened {
			_ = db.Close()
		}
	})
	return db
}

func activity161888Fill(t *testing.T, db *DB, bucket []byte, count, valueSize int) {
	t.Helper()
	err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			key := []byte(fmt.Sprintf("%08d", i))
			value := bytes.Repeat([]byte{byte(i%251 + 1)}, valueSize)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("fill database: %v", err)
	}
}

func activity161888RootPage(t *testing.T, db *DB, bucket []byte) common.Pgid {
	t.Helper()
	var root common.Pgid
	err := db.View(func(tx *Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return errors.New("bucket not found")
		}
		root = b.RootPage()
		return nil
	})
	if err != nil {
		t.Fatalf("read root page: %v", err)
	}
	if root < 2 {
		t.Fatalf("expected a materialized bucket root, got page %d", root)
	}
	return root
}

func activity161888FreelistOnDisk(t *testing.T, path string) []common.Pgid {
	t.Helper()
	meta, _, err := guts_cli.GetActiveMetaPage(path)
	if err != nil {
		t.Fatalf("read active meta page: %v", err)
	}
	p, _, err := guts_cli.ReadPage(path, uint64(meta.Freelist()))
	if err != nil {
		t.Fatalf("read freelist page: %v", err)
	}
	ids := append([]common.Pgid(nil), p.FreelistPageIds()...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func activity161888FreelistInMemory(db *DB) []common.Pgid {
	ids := make([]common.Pgid, db.freelist.Count())
	db.freelist.Copyall(ids)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func activity161888EqualPgids(a, b []common.Pgid) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestActivity161888CheckContainsMalformedPagePanic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "check.db")
	bucket := []byte("records")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	activity161888Fill(t, db, bucket, 500, 100)
	root := activity161888RootPage(t, db, bucket)
	if err := db.Close(); err != nil {
		t.Fatalf("close before corruption: %v", err)
	}

	p, buf, err := guts_cli.ReadPage(path, uint64(root))
	if err != nil {
		t.Fatalf("read root for corruption: %v", err)
	}
	if !p.IsBranchPage() {
		t.Fatalf("fixture root page %d is %s, want branch", root, p.Typ())
	}
	p.SetFlags(0)
	if err := guts_cli.WritePage(path, buf); err != nil {
		t.Fatalf("write malformed root page: %v", err)
	}

	db = activity161888Open(t, path, &Options{PageSize: 4096})
	var checkErrors []error
	err = db.View(func(tx *Tx) error {
		for checkErr := range tx.Check() {
			checkErrors = append(checkErrors, checkErr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view malformed database: %v", err)
	}
	if len(checkErrors) == 0 {
		t.Fatal("Tx.Check returned no diagnostic for a malformed root page")
	}
	var diagnostic strings.Builder
	for _, checkErr := range checkErrors {
		diagnostic.WriteString(checkErr.Error())
		diagnostic.WriteByte('\n')
	}
	if text := diagnostic.String(); !strings.Contains(text, "flags: 0") && !strings.Contains(text, "unexpected type/flags: 0") {
		t.Fatalf("Tx.Check diagnostic lost the malformed flags: %q", text)
	}
}

func TestActivity161888EmptyBranchStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.db")
	bucket := []byte("metrics")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	activity161888Fill(t, db, bucket, 500, 100)
	root := activity161888RootPage(t, db, bucket)
	if err := db.Close(); err != nil {
		t.Fatalf("close before branch edit: %v", err)
	}

	p, buf, err := guts_cli.ReadPage(path, uint64(root))
	if err != nil {
		t.Fatalf("read branch root: %v", err)
	}
	if !p.IsBranchPage() {
		t.Fatalf("fixture root page %d is %s, want branch", root, p.Typ())
	}
	p.SetCount(0)
	if err := guts_cli.WritePage(path, buf); err != nil {
		t.Fatalf("write zero-count branch: %v", err)
	}

	db = activity161888Open(t, path, &Options{PageSize: 4096})
	err = db.View(func(tx *Tx) error {
		stats := tx.Bucket(bucket).Stats()
		if stats.BranchPageN != 1 {
			return fmt.Errorf("BranchPageN=%d, want 1", stats.BranchPageN)
		}
		if stats.BranchInuse != int(common.PageHeaderSize) {
			return fmt.Errorf("BranchInuse=%d, want header-only %d", stats.BranchInuse, common.PageHeaderSize)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stats on zero-count branch: %v", err)
	}
}

func TestActivity161888RollbackReloadsFreelist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollback.db")
	bucket := []byte("records")
	db := activity161888Open(t, path, &Options{PageSize: 4096})

	keys := make([][]byte, 11)
	err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket(bucket)
		if err != nil {
			return err
		}
		for i := range keys {
			keys[i] = []byte(fmt.Sprintf("t1_k%02d", i))
			if err := b.Put(keys[i], bytes.Repeat([]byte{byte(i + 1)}, 1500)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("populate rollback fixture: %v", err)
	}
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket(bucket)
		for i := 0; i < 6; i++ {
			if err := b.Delete(keys[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("create free pages: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before failed transaction: %v", err)
	}
	db = activity161888Open(t, path, &Options{PageSize: 4096})
	before := activity161888FreelistOnDisk(t, path)
	if len(before) == 0 {
		t.Fatal("fixture did not create free pages")
	}

	originalWriteAt := db.ops.writeAt
	injected := errors.New("activity161888: injected meta publication failure")
	db.ops.writeAt = func(data []byte, offset int64) (int, error) {
		if offset == 0 || offset == int64(db.pageSize) {
			return 0, injected
		}
		return originalWriteAt(data, offset)
	}
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket(bucket)
		for i := 6; i < len(keys); i++ {
			if err := b.Put(keys[i], bytes.Repeat([]byte{byte(200 + i)}, 1500)); err != nil {
				return err
			}
		}
		return nil
	})
	db.ops.writeAt = originalWriteAt
	if !errors.Is(err, injected) {
		t.Fatalf("failed commit returned %v, want injected error", err)
	}

	afterDisk := activity161888FreelistOnDisk(t, path)
	afterMemory := activity161888FreelistInMemory(db)
	if !activity161888EqualPgids(before, afterDisk) {
		t.Fatalf("failed transaction changed committed freelist: before=%v after=%v", before, afterDisk)
	}
	if !activity161888EqualPgids(afterDisk, afterMemory) {
		t.Fatalf("in-memory freelist was not restored from committed state: disk=%v memory=%v", afterDisk, afterMemory)
	}

	err = db.Update(func(tx *Tx) error {
		return tx.Bucket(bucket).Put([]byte("recovery-write"), []byte("ok"))
	})
	if err != nil {
		t.Fatalf("next write after failed commit: %v", err)
	}
	err = db.View(func(tx *Tx) error {
		if got := tx.Bucket(bucket).Get([]byte("recovery-write")); !bytes.Equal(got, []byte("ok")) {
			return fmt.Errorf("recovery write value=%q", got)
		}
		for checkErr := range tx.Check() {
			return checkErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-recovery consistency: %v", err)
	}
}

func TestActivity161888MetaPublicationLocksReaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	bucket := []byte("records")
	db := activity161888Open(t, path, &Options{PageSize: 4096, InitialMmapSize: 1 << 20, NoSync: true})
	activity161888Fill(t, db, bucket, 10, 40)

	var before common.Txid
	err := db.View(func(tx *Tx) error {
		before = tx.meta.Txid()
		return nil
	})
	if err != nil {
		t.Fatalf("read initial txid: %v", err)
	}

	originalWriteAt := db.ops.writeAt
	entered := make(chan struct{})
	release := make(chan struct{})
	var blocked atomic.Bool
	db.ops.writeAt = func(data []byte, offset int64) (int, error) {
		if (offset == 0 || offset == int64(db.pageSize)) && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return originalWriteAt(data, offset)
	}

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- db.Update(func(tx *Tx) error {
			return tx.Bucket(bucket).Put([]byte("published"), []byte("value"))
		})
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		db.ops.writeAt = originalWriteAt
		t.Fatal("writer never reached meta publication")
	}

	type readResult struct {
		tx  *Tx
		err error
	}
	readerDone := make(chan readResult, 1)
	go func() {
		tx, err := db.Begin(false)
		readerDone <- readResult{tx: tx, err: err}
	}()

	var early *readResult
	select {
	case result := <-readerDone:
		early = &result
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-commitDone; err != nil {
		db.ops.writeAt = originalWriteAt
		if early != nil && early.tx != nil {
			_ = early.tx.Rollback()
		}
		t.Fatalf("writer commit: %v", err)
	}
	db.ops.writeAt = originalWriteAt

	if early != nil {
		if early.tx != nil {
			_ = early.tx.Rollback()
		}
		t.Fatal("read transaction began while the new meta page was only being published")
	}

	var result readResult
	select {
	case result = <-readerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not resume after meta publication")
	}
	if result.err != nil {
		t.Fatalf("begin reader after publication: %v", result.err)
	}
	if result.tx.meta.Txid() <= before {
		_ = result.tx.Rollback()
		t.Fatalf("reader observed txid %d, want newer than %d", result.tx.meta.Txid(), before)
	}
	if got := result.tx.Bucket(bucket).Get([]byte("published")); !bytes.Equal(got, []byte("value")) {
		_ = result.tx.Rollback()
		t.Fatalf("reader observed incomplete publication: %q", got)
	}
	if err := result.tx.Rollback(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
}
