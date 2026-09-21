// Package arch_test enforces the structural rules the design depends on.
// These are not style checks: each one prevents a class of defect that is
// expensive to undo once code is built on top of it.
package arch_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const modulePath = "github.com/unilinq/durableq"

// backendPackages are the storage adapters. Core runtime code must never
// import one: the moment it does, a second backend stops being additive.
var backendPackages = []string{
	modulePath + "/storage/postgres",
	modulePath + "/storage/sqlite",
}

// backendImportAllowed lists the packages that may legitimately import an
// adapter: the adapter itself, the wiring in cmd, the test harness, and
// examples that must name a concrete backend to run.
func backendImportAllowed(pkg string) bool {
	switch {
	case pkg == modulePath+"/storage/postgres",
		pkg == modulePath+"/storage/sqlite":
		return true
	case strings.HasPrefix(pkg, modulePath+"/internal/dqtest"):
		// The test harness and its crash-test helper must name a backend.
		return true
	case strings.HasPrefix(pkg, modulePath+"/cmd/"),
		strings.HasPrefix(pkg, modulePath+"/examples/"):
		return true
	}
	return false
}

// TestCoreDoesNotImportABackend walks every package's imports and fails if a
// core package reaches for a concrete storage adapter.
func TestCoreDoesNotImportABackend(t *testing.T) {
	t.Parallel()
	for pkg, imports := range packageImports(t) {
		if backendImportAllowed(pkg) {
			continue
		}
		for _, imp := range imports {
			for _, backend := range backendPackages {
				if imp == backend {
					t.Errorf("package %s imports %s\n"+
						"Core packages must depend on the storage contract only. "+
						"If this import is legitimate, add the package to backendImportAllowed "+
						"with a reason.", pkg, backend)
				}
			}
		}
	}
}

// TestStorageTestIsBackendAgnostic guards the conformance suite specifically:
// it is the thing that makes a second adapter cheap, so it must not know about
// any particular one.
func TestStorageTestIsBackendAgnostic(t *testing.T) {
	t.Parallel()
	imports := packageImports(t)[modulePath+"/storagetest"]
	for _, imp := range append(imports, packageImports(t)[modulePath+"/storage/storagetest"]...) {
		for _, backend := range backendPackages {
			if imp == backend {
				t.Errorf("storagetest imports %s; the conformance suite must stay backend-agnostic", backend)
			}
		}
	}
}

// TestNoSleepForSynchronisation enforces the rule that concurrent tests wait on
// a signal or an injected clock, never on the wall clock. A sleep that is
// genuinely about elapsed real time can opt out with a marker comment naming
// the reason.
func TestNoSleepForSynchronisation(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	const marker = "durableq:allow-sleep"
	// Built at runtime so this checker does not match its own source.
	needle := "time." + "Sleep("
	selfFile := filepath.Join("internal", "arch", "arch_test.go")

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "bin" || d.Name() == "dist" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if rel, err := filepath.Rel(root, path); err == nil && rel == selfFile {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		for i, line := range strings.Split(string(body), "\n") {
			if !strings.Contains(line, needle) {
				continue
			}
			if strings.Contains(line, marker) {
				continue
			}
			t.Errorf("%s:%d uses time.Sleep for synchronisation:\n\t%s\n"+
				"Wait on a dqsignal or advance the injected clock instead. "+
				"If the sleep is genuinely about elapsed real time, append a "+
				"// %s comment naming the reason.", rel, i+1, strings.TrimSpace(line), marker)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repo: %v", err)
	}
}

// packageImports returns every package in the module and what it imports,
// including its test files.
func packageImports(t *testing.T) map[string][]string {
	t.Helper()
	cmd := exec.Command("go", "list", "-e",
		"-f", "{{.ImportPath}}\t{{join .Imports \",\"}},{{join .TestImports \",\"}},{{join .XTestImports \",\"}}",
		"./...")
	cmd.Dir = repoRoot(t)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	result := map[string][]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		pkg, imports, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		var cleaned []string
		for _, imp := range strings.Split(imports, ",") {
			if imp != "" {
				cleaned = append(cleaned, imp)
			}
		}
		result[pkg] = cleaned
	}
	return result
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if dir == filepath.Dir(dir) {
			t.Fatalf("no go.mod above %s", wd)
		}
	}
}
