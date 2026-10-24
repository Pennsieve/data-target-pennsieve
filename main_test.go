package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizePathPrefix(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{"", "", false},
		{"/", "", false},
		{"qc", "qc", false},
		{" /qc/plots/ ", "qc/plots", false},
		{"../x", "", true},
		{"qc/../x", "", true},
		{"./qc", "", true},
		{"qc//plots", "", true},
		{`qc\plots`, "", true},
	}
	for _, tc := range cases {
		got, err := normalizePathPrefix(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("normalizePathPrefix(%q) = %q, want error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("normalizePathPrefix(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
}

func TestSelectFiles(t *testing.T) {
	dir := t.TempDir()
	files := []string{"a.png", "qc/b.png", "qc/sub/c.png", "qcx/d.png", "data/e.csv"}
	var paths []string
	for _, f := range files {
		p := filepath.Join(dir, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}

	rels := func(sel []selectedFile) []string {
		var out []string
		for _, s := range sel {
			out = append(out, s.rel)
		}
		return out
	}
	equal := func(a, b []string) bool {
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

	if got := rels(selectFiles(dir, "", paths)); !equal(got, files) {
		t.Errorf("no prefix: got %v, want %v", got, files)
	}
	if got, want := rels(selectFiles(dir, "qc", paths)), []string{"b.png", "sub/c.png"}; !equal(got, want) {
		t.Errorf("prefix qc: got %v, want %v", got, want)
	}
	if got, want := rels(selectFiles(dir, "qc/sub", paths)), []string{"c.png"}; !equal(got, want) {
		t.Errorf("prefix qc/sub: got %v, want %v", got, want)
	}
	if got := selectFiles(dir, "nothing", paths); len(got) != 0 {
		t.Errorf("prefix nothing: got %v, want none", rels(got))
	}
	// A prefix naming one file selects it, under its own name.
	if got, want := rels(selectFiles(dir, "qc/sub/c.png", paths)), []string{"c.png"}; !equal(got, want) {
		t.Errorf("prefix qc/sub/c.png: got %v, want %v", got, want)
	}
}

func TestBridgeParams(t *testing.T) {
	t.Setenv("PATH_PREFIX", "stale")       // from a previous invocation
	t.Setenv("PENNSIEVE_API_HOST2", "api") // a function setting
	bridgeParams(map[string]string{"TARGET_FOLDER": "out", "PENNSIEVE_API_HOST2": "https://attacker.example", "DATASET_ID": "N:dataset:other"})
	if got := os.Getenv("TARGET_FOLDER"); got != "out" {
		t.Errorf("TARGET_FOLDER = %q, want out", got)
	}
	if _, set := os.LookupEnv("PATH_PREFIX"); set {
		t.Error("PATH_PREFIX from the previous invocation is still set")
	}
	if got := os.Getenv("PENNSIEVE_API_HOST2"); got != "api" {
		t.Errorf("PENNSIEVE_API_HOST2 = %q, a param overrode a function setting", got)
	}
	if _, set := os.LookupEnv("DATASET_ID"); set {
		t.Error("DATASET_ID was set from a param")
	}
}
