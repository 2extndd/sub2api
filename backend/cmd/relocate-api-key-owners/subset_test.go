package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadExactSubsetManifest(t *testing.T) {
	value := exactSubsetFixture()
	path := writeManifestFixture(t, value)

	loaded, checksum, err := loadManifest(path)
	if err != nil {
		t.Fatalf("load subset manifest: %v", err)
	}
	if len(checksum) != sha256.Size*2 {
		t.Fatalf("checksum length=%d", len(checksum))
	}
	if loaded.Summary.Total != requiredSubsetSize || loaded.Summary.Transfer != requiredSubsetSize {
		t.Fatalf("unexpected derived summary: %+v", loaded.Summary)
	}
	selected, summary, err := selectEntries(loaded, nil, runOptions{BatchSize: requiredSubsetSize, Direction: "forward"})
	if err != nil {
		t.Fatalf("select subset: %v", err)
	}
	if len(selected) != requiredSubsetSize || summary.Selected != requiredSubsetSize {
		t.Fatalf("selected=%d summary=%+v", len(selected), summary)
	}
	for i := 1; i < len(selected); i++ {
		if *selected[i-1].RemoteKeyID >= *selected[i].RemoteKeyID {
			t.Fatal("subset is not in stable remote-ID order")
		}
	}
}

func TestExactSubsetRequiresChecksumAndForbidsPartialSelection(t *testing.T) {
	value := exactSubsetFixture()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	checksum := hex.EncodeToString(sum[:])
	base := runOptions{BatchSize: requiredSubsetSize, Direction: "forward"}

	_, err = run(context.Background(), nil, nil, value, checksum, nil, base)
	if err == nil || !strings.Contains(err.Error(), "manifest-sha256") {
		t.Fatalf("missing checksum error=%v", err)
	}

	base.ExpectedChecksum = checksum
	base.OnlyID = 1001
	_, err = run(context.Background(), nil, nil, value, checksum, nil, base)
	if err == nil || !strings.Contains(err.Error(), "forbids") {
		t.Fatalf("partial selection error=%v", err)
	}
}

func TestExactSubsetRejectsWrongCountAndDuplicateOneProviderID(t *testing.T) {
	wrongCount := exactSubsetFixture()
	wrongCount.Entries = wrongCount.Entries[:7]
	wrongCount.EntryCount = 7
	if _, _, err := loadManifest(writeManifestFixture(t, wrongCount)); err == nil || !strings.Contains(err.Error(), "exactly 8") {
		t.Fatalf("wrong count error=%v", err)
	}

	duplicate := exactSubsetFixture()
	duplicate.Entries[7].OneProviderKeyID = duplicate.Entries[0].OneProviderKeyID
	if _, _, err := loadManifest(writeManifestFixture(t, duplicate)); err == nil || !strings.Contains(err.Error(), "duplicate oneprovider_key_id") {
		t.Fatalf("duplicate ID error=%v", err)
	}
}

func exactSubsetFixture() *manifest {
	entries := make([]manifestEntry, 0, requiredSubsetSize)
	for i := 0; i < requiredSubsetSize; i++ {
		remoteID := int64(1008 - i)
		currentUserID := int64(200 + i)
		fingerprintValue := "fingerprint-" + string(rune('a'+i))
		protectedValue := "protected-" + string(rune('a'+i))
		entries = append(entries, manifestEntry{
			Action:           "transfer",
			CurrentUserID:    &currentUserID,
			ExpectedUserID:   int64(300 + i),
			Fingerprint:      &fingerprintValue,
			OneProviderKeyID: int64(400 + i),
			ProtectedDigest:  &protectedValue,
			RemoteKeyID:      &remoteID,
			SourceCabinetID:  500,
			TargetCabinetID:  600,
		})
	}
	return &manifest{
		Schema:                  subsetManifestSchema,
		SensitiveValuesIncluded: false,
		SourceProvider:          "api2cn",
		TargetProvider:          "onesubprovider",
		EntryCount:              requiredSubsetSize,
		MappingDigest:           "mapping-digest",
		Entries:                 entries,
	}
}

func writeManifestFixture(t *testing.T, value *manifest) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
