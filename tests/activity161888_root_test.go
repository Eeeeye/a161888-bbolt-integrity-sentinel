package bbolt

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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

func TestActivity161888CheckPreservesValidDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valid-check.db")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	activity161888Fill(t, db, []byte("records"), 500, 100)
	activity161888Fill(t, db, []byte("archive"), 40, 40)

	err := db.View(func(tx *Tx) error {
		var diagnostics []string
		for checkErr := range tx.Check() {
			diagnostics = append(diagnostics, checkErr.Error())
		}
		if len(diagnostics) != 0 {
			return fmt.Errorf("valid database produced diagnostics: %v", diagnostics)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("check valid database: %v", err)
	}
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

func TestActivity161888CheckPreservesKeyOrderDiagnostics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key-order.db")
	bucket := []byte("records")
	db := activity161888Open(t, path, &Options{PageSize: 4096})
	activity161888Fill(t, db, bucket, 500, 100)
	root := activity161888RootPage(t, db, bucket)
	if err := db.Close(); err != nil {
		t.Fatalf("close before key corruption: %v", err)
	}

	pageID := root
	for {
		p, buf, err := guts_cli.ReadPage(path, uint64(pageID))
		if err != nil {
			t.Fatalf("read page %d while locating leaf: %v", pageID, err)
		}
		switch {
		case p.IsBranchPage():
			if p.Count() == 0 {
				t.Fatalf("branch page %d has no children", pageID)
			}
			pageID = p.BranchPageElement(0).Pgid()
		case p.IsLeafPage():
			if p.Count() < 2 {
				t.Fatalf("leaf page %d has %d elements, want at least 2", pageID, p.Count())
			}
			secondKey := p.LeafPageElement(1).Key()
			if len(secondKey) == 0 {
				t.Fatalf("leaf page %d has an empty second key", pageID)
			}
			secondKey[0] = 0
			if err := guts_cli.WritePage(path, buf); err != nil {
				t.Fatalf("write out-of-order leaf page %d: %v", pageID, err)
			}
			goto corrupted
		default:
			t.Fatalf("page %d is %s while locating a leaf", pageID, p.Typ())
		}
	}

corrupted:
	db = activity161888Open(t, path, &Options{PageSize: 4096})
	var diagnostics []string
	err := db.View(func(tx *Tx) error {
		for checkErr := range tx.Check() {
			diagnostics = append(diagnostics, checkErr.Error())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view database with out-of-order key: %v", err)
	}
	if len(diagnostics) == 0 {
		t.Fatal("Tx.Check returned no key-order diagnostic")
	}
	joined := strings.Join(diagnostics, "\n")
	if !strings.Contains(joined, "needs to be >") ||
		!strings.Contains(joined, "previous element") ||
		!strings.Contains(joined, "Stack") {
		t.Fatalf("Tx.Check lost the key-order diagnostic context: %q", joined)
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

func TestActivity161888RollbackPreservesNoFreelistSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollback-no-freelist-sync.db")
	bucket := []byte("records")
	options := &Options{PageSize: 4096, NoFreelistSync: true}
	db := activity161888Open(t, path, options)

	keys := make([][]byte, 11)
	err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket(bucket)
		if err != nil {
			return err
		}
		for i := range keys {
			keys[i] = []byte(fmt.Sprintf("nfs_k%02d", i))
			if err := b.Put(keys[i], bytes.Repeat([]byte{byte(i + 1)}, 1500)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("populate no-freelist-sync fixture: %v", err)
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
		t.Fatalf("create no-freelist-sync free pages: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close no-freelist-sync fixture: %v", err)
	}
	db = activity161888Open(t, path, options)
	if got := len(activity161888FreelistInMemory(db)); got == 0 {
		t.Fatal("no-freelist-sync fixture reconstructed no free pages")
	}
	meta, _, err := guts_cli.GetActiveMetaPage(path)
	if err != nil {
		t.Fatalf("read no-freelist-sync meta page: %v", err)
	}
	if meta.Freelist() != common.PgidNoFreelist {
		t.Fatalf("freelist page=%d, want no-freelist marker %d", meta.Freelist(), common.PgidNoFreelist)
	}

	originalWriteAt := db.ops.writeAt
	injected := errors.New("activity161888: injected no-freelist meta publication failure")
	db.ops.writeAt = func(data []byte, offset int64) (int, error) {
		if offset == 0 || offset == int64(db.pageSize) {
			return 0, injected
		}
		return originalWriteAt(data, offset)
	}
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket(bucket)
		for i := 0; i < 8; i++ {
			key := []byte(fmt.Sprintf("failed-write-%02d", i))
			if err := b.Put(key, bytes.Repeat([]byte{byte(220 + i)}, 1800)); err != nil {
				return err
			}
		}
		return nil
	})
	db.ops.writeAt = originalWriteAt
	if !errors.Is(err, injected) {
		t.Fatalf("failed no-freelist-sync commit returned %v, want injected error", err)
	}
	authoritative := db.freepages()
	sort.Slice(authoritative, func(i, j int) bool { return authoritative[i] < authoritative[j] })
	afterMemory := activity161888FreelistInMemory(db)
	if len(authoritative) == 0 {
		t.Fatal("committed page graph reconstructed no free pages after failed transaction")
	}
	if !activity161888EqualPgids(authoritative, afterMemory) {
		t.Fatalf("no-freelist-sync rollback did not restore committed free pages: graph=%v memory=%v", authoritative, afterMemory)
	}

	err = db.View(func(tx *Tx) error {
		b := tx.Bucket(bucket)
		for i := 0; i < 8; i++ {
			if got := b.Get([]byte(fmt.Sprintf("failed-write-%02d", i))); got != nil {
				return fmt.Errorf("failed transaction published key %d: %q", i, got)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("committed view after no-freelist-sync failure: %v", err)
	}
	err = db.Update(func(tx *Tx) error {
		return tx.Bucket(bucket).Put([]byte("recovery-write"), []byte("ok"))
	})
	if err != nil {
		t.Fatalf("next no-freelist-sync write after failed commit: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close recovered no-freelist-sync database: %v", err)
	}

	db = activity161888Open(t, path, options)
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket(bucket)
		if got := b.Get([]byte("recovery-write")); !bytes.Equal(got, []byte("ok")) {
			return fmt.Errorf("recovery write value=%q", got)
		}
		for i := 0; i < 8; i++ {
			if got := b.Get([]byte(fmt.Sprintf("failed-write-%02d", i))); got != nil {
				return fmt.Errorf("reopened database contains failed key %d: %q", i, got)
			}
		}
		for checkErr := range tx.Check() {
			return checkErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reopened no-freelist-sync consistency: %v", err)
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

func TestActivity161888MetaPublicationLocksThroughDurability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta-durability.db")
	bucket := []byte("records")
	db := activity161888Open(t, path, &Options{PageSize: 4096, InitialMmapSize: 1 << 20})
	activity161888Fill(t, db, bucket, 10, 40)

	originalSyncMeta := db.ops.syncMeta
	entered := make(chan struct{})
	release := make(chan struct{})
	var blocked atomic.Bool
	db.ops.syncMeta = func() error {
		if blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return originalSyncMeta()
	}
	t.Cleanup(func() { db.ops.syncMeta = originalSyncMeta })

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- db.Update(func(tx *Tx) error {
			return tx.Bucket(bucket).Put([]byte("durable"), []byte("value"))
		})
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("writer never reached the metadata durability boundary")
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

	select {
	case result := <-readerDone:
		if result.tx != nil {
			_ = result.tx.Rollback()
		}
		t.Fatal("read transaction began before metadata durability completed")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if err := <-commitDone; err != nil {
		t.Fatalf("writer commit: %v", err)
	}

	var result readResult
	select {
	case result = <-readerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not resume after metadata durability completed")
	}
	if result.err != nil {
		t.Fatalf("begin reader after durability: %v", result.err)
	}
	if got := result.tx.Bucket(bucket).Get([]byte("durable")); !bytes.Equal(got, []byte("value")) {
		_ = result.tx.Rollback()
		t.Fatalf("reader observed incomplete durable publication: %q", got)
	}
	if err := result.tx.Rollback(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
}

func TestActivity161888MetaShortWriteFailsAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta-short-write.db")
	bucket := []byte("records")
	db := activity161888Open(t, path, &Options{PageSize: 4096, InitialMmapSize: 1 << 20, NoSync: true})
	activity161888Fill(t, db, bucket, 10, 40)

	originalWriteAt := db.ops.writeAt
	var injected atomic.Bool
	db.ops.writeAt = func(data []byte, offset int64) (int, error) {
		if (offset == 0 || offset == int64(db.pageSize)) && injected.CompareAndSwap(false, true) {
			return len(data) - 1, nil
		}
		return originalWriteAt(data, offset)
	}
	err := db.Update(func(tx *Tx) error {
		return tx.Bucket(bucket).Put([]byte("short-write"), []byte("must-not-commit"))
	})
	db.ops.writeAt = originalWriteAt
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short metadata write returned %v, want %v", err, io.ErrShortWrite)
	}

	if err := db.View(func(tx *Tx) error {
		if got := tx.Bucket(bucket).Get([]byte("short-write")); got != nil {
			return fmt.Errorf("short metadata write published value %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("view after short metadata write: %v", err)
	}
	if err := db.Update(func(tx *Tx) error {
		return tx.Bucket(bucket).Put([]byte("recovery"), []byte("ok"))
	}); err != nil {
		t.Fatalf("commit after short metadata write: %v", err)
	}
}

func TestActivity161888MetaSyncErrorReleasesReaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta-sync-error.db")
	bucket := []byte("records")
	db := activity161888Open(t, path, &Options{PageSize: 4096, InitialMmapSize: 1 << 20})
	activity161888Fill(t, db, bucket, 10, 40)

	originalSyncMeta := db.ops.syncMeta
	injected := errors.New("activity161888: injected metadata sync failure")
	var failed atomic.Bool
	db.ops.syncMeta = func() error {
		if failed.CompareAndSwap(false, true) {
			return injected
		}
		return originalSyncMeta()
	}
	err := db.Update(func(tx *Tx) error {
		return tx.Bucket(bucket).Put([]byte("sync-failed"), []byte("complete-value"))
	})
	db.ops.syncMeta = originalSyncMeta
	if !errors.Is(err, injected) {
		t.Fatalf("metadata sync failure returned %v, want injected error", err)
	}

	viewDone := make(chan error, 1)
	go func() {
		viewDone <- db.View(func(tx *Tx) error {
			if got := tx.Bucket(bucket).Get([]byte("sync-failed")); got != nil && !bytes.Equal(got, []byte("complete-value")) {
				return fmt.Errorf("metadata sync failure exposed a partial value %q", got)
			}
			for checkErr := range tx.Check() {
				return checkErr
			}
			return nil
		})
	}()
	select {
	case err := <-viewDone:
		if err != nil {
			t.Fatalf("view after metadata sync failure: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("metadata sync failure left readers blocked")
	}

	if err := db.Update(func(tx *Tx) error {
		return tx.Bucket(bucket).Put([]byte("recovery-after-sync"), []byte("ok"))
	}); err != nil {
		t.Fatalf("commit after metadata sync failure: %v", err)
	}
}
