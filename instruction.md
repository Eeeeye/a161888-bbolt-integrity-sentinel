# bbolt integrity sentinel

You are maintaining an embedded key-value store used by a recovery service. A recent refactor introduced several independent integrity and concurrency regressions. Repair the implementation in `/app` without replacing the module or changing its public API.

## Required behavior

1. The bundled database inspection facility that searches page paths for a key must terminate when a reachable branch-page graph contains a cycle. It must return an error that identifies the repeated page and includes the active traversal stack. A valid database containing the same key in distinct buckets must still return every legitimate path.
2. A transaction-wide consistency scan must contain panics raised while traversing malformed pages. Its diagnostic stream must yield an error and then close; a malformed page must not crash the process. Existing checks on valid databases and key-order diagnostics must remain intact.
3. Bucket statistics reporting must handle a branch page whose element count is zero without panicking or reading before the page buffer. Such a page still counts as one branch page, and its in-use size is exactly the page-header size. Normal branch statistics must remain unchanged.
4. If a writable transaction has written dirty pages but commit-metadata publication fails, rollback must restore the in-memory free-page catalog from the last committed on-disk state. The failed transaction must not change committed data or the committed catalog, and the next write plus a full consistency scan must succeed. Preserve both the mode that persists the catalog and the mode that rebuilds it from committed pages.
5. Meta-page publication must be synchronized with creation of read-only snapshots. A reader that starts while a writer is inside the meta-page write must wait until publication finishes, then observe the complete, monotonically newer transaction state. Preserve error propagation from the underlying write.
6. A cursor exhausted by repeated reverse steps must remain positioned at the first element so that a subsequent forward step resumes at the second element. Selecting the last entry of an empty bucket must remain safe and return nil key and value results.
7. Both streaming and copy-to-path exports from a read transaction must copy the file snapshot on which that transaction was opened, even if the database path is atomically replaced before copying. Concurrent exports must use independent read offsets, and a nonzero caller-supplied open flag must not silently switch to another inode.
8. A commit that advances the high-water mark must grow the data file before writing dirty pages whether the free-page catalog is persisted or rebuilt. Omitting the catalog from commits must not bypass growth, and crossing the allocation-size boundary must retain chunked growth semantics.
9. Opening a database with one corrupt meta page must recover from the other valid committed meta page, choosing the newest valid transaction rather than the numerically newest invalid one. Opening must still fail when neither meta page is valid.

## Constraints

- Keep the module path `go.etcd.io/bbolt` and the existing exported API.
- Make production fixes in the existing source tree. Do not fetch or substitute a pre-repaired repository.
- Do not modify evaluator tests, `/tests`, or reward files.
- The final implementation must build with Go 1.25 and must not require network access during verification.

All nine behaviors are required. Verification is closed and binary: any missing behavior receives reward `0`; the complete repair receives reward `1`.
