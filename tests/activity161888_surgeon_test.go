package surgeon

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func activity161888CreateXRayDB(t *testing.T, path string, buckets ...[]byte) {
	t.Helper()
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open XRay fixture: %v", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range buckets {
			b, err := tx.CreateBucket(name)
			if err != nil {
				return err
			}
			for i := 0; i < 500; i++ {
				key := []byte(fmt.Sprintf("%04d", i))
				if err := b.Put(key, bytes.Repeat([]byte{byte(i%251 + 1)}, 100)); err != nil {
					return err
				}
			}
			if err := b.Put([]byte("shared"), []byte("value")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("fill XRay fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close XRay fixture: %v", err)
	}
}

func TestActivity161888XRayRejectsPageCycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cycle.db")
	activity161888CreateXRayDB(t, path, []byte("records"))

	paths, err := NewXRay(path).FindPathsToKey([]byte("0001"))
	if err != nil {
		t.Fatalf("find initial path: %v", err)
	}
	if len(paths) == 0 || len(paths[0]) < 2 {
		t.Fatalf("fixture path is too short: %v", paths)
	}
	pathToKey := paths[0]
	ancestor := pathToKey[len(pathToKey)-2]
	leaf := pathToKey[len(pathToKey)-1]
	ancestorPage, _, err := guts_cli.ReadPage(path, uint64(ancestor))
	if err != nil {
		t.Fatalf("read ancestor page: %v", err)
	}
	if !ancestorPage.IsBranchPage() {
		t.Fatalf("ancestor page %d is %s, want branch", ancestor, ancestorPage.Typ())
	}
	var referencesLeaf bool
	for i := uint16(0); i < ancestorPage.Count(); i++ {
		if ancestorPage.BranchPageElement(i).Pgid() == common.Pgid(leaf) {
			referencesLeaf = true
			break
		}
	}
	if !referencesLeaf {
		t.Fatalf("ancestor %d does not reference leaf %d", ancestor, leaf)
	}
	if err := CopyPage(path, ancestor, leaf); err != nil {
		t.Fatalf("create page cycle: %v", err)
	}

	_, err = NewXRay(path).FindPathsToKey([]byte("0001"))
	if err == nil {
		t.Fatal("FindPathsToKey accepted a reachable branch-page cycle")
	}
	diagnostic := err.Error()
	if !strings.Contains(diagnostic, "cycle detected") || !strings.Contains(diagnostic, "stack") || !strings.Contains(diagnostic, fmt.Sprint(leaf)) {
		t.Fatalf("cycle diagnostic lacks the repeated page and traversal stack: %q", diagnostic)
	}
}

func TestActivity161888XRayPreservesDistinctBucketPaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buckets.db")
	activity161888CreateXRayDB(t, path, []byte("bucket-a"), []byte("bucket-b"))

	paths, err := NewXRay(path).FindPathsToKey([]byte("shared"))
	if err != nil {
		t.Fatalf("find shared key: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("found %d shared-key paths, want one per bucket: %v", len(paths), paths)
	}
	leafs := make(map[common.Pgid]struct{}, 2)
	for _, found := range paths {
		if len(found) == 0 {
			t.Fatal("XRay returned an empty path")
		}
		leafs[found[len(found)-1]] = struct{}{}
	}
	if len(leafs) != 2 {
		t.Fatalf("shared-key paths terminate on %d distinct leaves, want 2", len(leafs))
	}
}

func TestActivity161888XRayDoesNotConfuseSharedChildWithCycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared-child.db")
	activity161888CreateXRayDB(t, path, []byte("records"))

	initial, err := NewXRay(path).FindPathsToKey([]byte("0001"))
	if err != nil {
		t.Fatalf("find initial path: %v", err)
	}
	if len(initial) != 1 || len(initial[0]) < 3 {
		t.Fatalf("fixture path is not a materialized bucket path: %v", initial)
	}
	pathToKey := initial[0]
	branchID := pathToKey[len(pathToKey)-2]
	leafID := pathToKey[len(pathToKey)-1]
	branch, buf, err := guts_cli.ReadPage(path, uint64(branchID))
	if err != nil {
		t.Fatalf("read parent branch page: %v", err)
	}
	if !branch.IsBranchPage() || branch.Count() < 2 {
		t.Fatalf("parent page %d is %s with %d elements, want a multi-child branch", branchID, branch.Typ(), branch.Count())
	}

	originalIndex := -1
	for i := uint16(0); i < branch.Count(); i++ {
		if branch.BranchPageElement(i).Pgid() == leafID {
			originalIndex = int(i)
			break
		}
	}
	if originalIndex < 0 {
		t.Fatalf("branch page %d does not reference leaf %d", branchID, leafID)
	}
	sharedIndex := (originalIndex + 1) % int(branch.Count())
	branch.BranchPageElement(uint16(sharedIndex)).SetPgid(leafID)
	if err := guts_cli.WritePage(path, buf); err != nil {
		t.Fatalf("write acyclic shared-child graph: %v", err)
	}

	paths, err := NewXRay(path).FindPathsToKey([]byte("0001"))
	if err != nil {
		t.Fatalf("acyclic shared child was reported as a cycle: %v", err)
	}
	var matching int
	for _, found := range paths {
		if len(found) != 0 && found[len(found)-1] == leafID {
			matching++
		}
	}
	if matching != 2 {
		t.Fatalf("shared child produced %d matching routes, want 2: %v", matching, paths)
	}
}
