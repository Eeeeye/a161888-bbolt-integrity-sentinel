# bbolt integrity sentinel

You are maintaining an embedded key-value store used by a recovery service. A recent refactor introduced several independent integrity and concurrency regressions. Repair the implementation in `/app` without replacing the module or changing its public API.

## Required behavior

1. `internal/surgeon.XRay` must terminate when a reachable branch-page graph contains a cycle. `FindPathsToKey` must return an error that identifies the repeated page and includes the traversal stack. A valid database containing the same key in distinct buckets must still return every legitimate path.
2. `Tx.Check` must contain panics raised while traversing malformed pages. The returned error channel must yield a diagnostic and then close; a malformed page must not crash the process. Existing checks on valid databases and key-order diagnostics must remain intact.
3. `Bucket.Stats` must handle a branch page whose element count is zero without panicking or reading before the page buffer. Such a page still counts as one branch page, and its in-use size is exactly the page-header size. Normal branch statistics must remain unchanged.
4. If a writable transaction has written dirty pages but meta-page publication fails, rollback must restore the in-memory freelist from the last committed on-disk state. The failed transaction must not change committed data or the committed freelist, and the next write plus `Tx.Check` must succeed. Preserve both synced-freelist and no-freelist-sync behavior.
5. Meta-page publication must be synchronized with creation of read-only snapshots. A reader that starts while a writer is inside the meta-page write must wait until publication finishes, then observe the complete, monotonically newer transaction state. Preserve error propagation from the underlying write.
6. A cursor exhausted with repeated `Prev` calls must remain positioned at the first element so that a subsequent `Next` resumes at the second element. `Last` on an empty bucket must remain safe and return nil values.
7. `Tx.WriteTo` and `CopyFile` must copy the file snapshot on which the read transaction was opened, even if the database path is atomically replaced before copying. Concurrent copies must use independent read offsets, and a nonzero `WriteFlag` must not silently switch to another inode.
8. A commit that advances the high-water mark must grow the data file before writing dirty pages in both freelist modes. `NoFreelistSync` must not bypass growth, and crossing the allocation-size boundary must retain chunked growth semantics.
9. Opening a database with one corrupt meta page must recover from the other valid committed meta page, choosing the newest valid transaction rather than the numerically newest invalid one. Opening must still fail when neither meta page is valid.

## Constraints

- Keep the module path `go.etcd.io/bbolt` and the existing exported API.
- Make production fixes in the existing source tree. Do not fetch or substitute a pre-repaired repository.
- Do not modify evaluator tests, `/tests`, or reward files.
- The final implementation must build with Go 1.25 and must not require network access during verification.

All nine behaviors are required. Verification is closed and binary: any missing behavior receives reward `0`; the complete repair receives reward `1`.
