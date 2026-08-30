package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProtectedDigestMatchesPythonCanonicalFixture(t *testing.T) {
	t.Parallel()
	row := keyRow{
		Key:           "test-credential-not-secret",
		Quota:         10.5,
		QuotaUsed:     2.25,
		ExpiresAt:     sql.NullTime{Time: mustTime(t, "2026-09-01T00:00:00Z"), Valid: true},
		GroupID:       sql.NullInt64{Int64: 8, Valid: true},
		Status:        "active",
		IPBlacklist:   json.RawMessage(`[]`),
		IPWhitelist:   json.RawMessage(`["127.0.0.1"]`),
		RateLimit5h:   1,
		RateLimit1d:   3,
		RateLimit7d:   5,
		Usage5h:       0.25,
		Usage1d:       0.5,
		Usage7d:       0.75,
		Window5hStart: sql.NullTime{},
		Window1dStart: sql.NullTime{Time: mustTime(t, "2026-08-27T00:00:00Z"), Valid: true},
		Window7dStart: sql.NullTime{Time: mustTime(t, "2026-08-20T00:00:00Z"), Valid: true},
	}
	got, err := protectedDigest(row)
	if err != nil {
		t.Fatal(err)
	}
	const want = "b63d9dce12d0e358889f7d60bbc8d5827361fce7d5a7cbb29fd279194e882e6b"
	if got != want {
		t.Fatalf("protected digest mismatch: got %s want %s", got, want)
	}
	if got := fingerprint(row.Key); got != "cc87d033ffdc412a4fbeb573" {
		t.Fatalf("fingerprint mismatch: %s", got)
	}
}

func TestProtectedDigestPreservesNullVersusEmptyIPLists(t *testing.T) {
	t.Parallel()
	base := keyRow{Key: "fixture", Status: "active", IPBlacklist: json.RawMessage(`null`), IPWhitelist: json.RawMessage(`null`)}
	nullDigest, err := protectedDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	base.IPBlacklist = json.RawMessage(`[]`)
	base.IPWhitelist = json.RawMessage(`[]`)
	emptyDigest, err := protectedDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	if nullDigest == emptyDigest {
		t.Fatal("protected digest must distinguish JSON null from empty arrays")
	}
}

func TestValidateManifestSummaryAndSelection(t *testing.T) {
	t.Parallel()
	fp1, fp2 := "fingerprint-one", "fingerprint-two"
	pd1, pd2 := "protected-one", "protected-two"
	old1, old2 := int64(24), int64(42)
	id1, id2 := int64(100), int64(200)
	value := &manifest{
		Schema:                  manifestSchema,
		SourceProvider:          "api2cn",
		TargetProvider:          "onesubprovider",
		SensitiveValuesIncluded: false,
		Summary:                 manifestSummary{Total: 3, Transfer: 2, Correct: 1},
		Entries: []manifestEntry{
			{Action: "transfer", OneProviderKeyID: 2, RemoteKeyID: &id2, CurrentUserID: &old2, ExpectedUserID: 21, Fingerprint: &fp2, ProtectedDigest: &pd2},
			{Action: "correct", OneProviderKeyID: 3, RemoteKeyID: ptrInt64(300), CurrentUserID: ptrInt64(30), ExpectedUserID: 30, Fingerprint: ptrString("fingerprint-three"), ProtectedDigest: ptrString("protected-three")},
			{Action: "transfer", OneProviderKeyID: 1, RemoteKeyID: &id1, CurrentUserID: &old1, ExpectedUserID: 20, Fingerprint: &fp1, ProtectedDigest: &pd1},
		},
	}
	if err := validateManifestSummary(value); err != nil {
		t.Fatal(err)
	}
	selected, summary, err := selectEntries(value, map[int64]struct{}{id2: {}}, runOptions{BatchSize: 25})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || *selected[0].RemoteKeyID != id1 {
		t.Fatalf("unexpected selection: %+v", selected)
	}
	if summary.Excluded != 1 || summary.Selected != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestManifestRejectsDuplicateRemoteID(t *testing.T) {
	t.Parallel()
	id := int64(100)
	old := int64(24)
	value := &manifest{
		Summary: manifestSummary{Total: 2, Transfer: 2},
		Entries: []manifestEntry{
			{Action: "transfer", OneProviderKeyID: 1, RemoteKeyID: &id, CurrentUserID: &old, ExpectedUserID: 20, Fingerprint: ptrString("fp-1"), ProtectedDigest: ptrString("pd-1")},
			{Action: "transfer", OneProviderKeyID: 2, RemoteKeyID: &id, CurrentUserID: &old, ExpectedUserID: 22, Fingerprint: ptrString("fp-2"), ProtectedDigest: ptrString("pd-2")},
		},
	}
	if err := validateManifestSummary(value); err == nil || !strings.Contains(err.Error(), "duplicate remote_key_id") {
		t.Fatalf("expected duplicate remote ID error, got %v", err)
	}
}

func TestLoadExclusionsFailsClosedAndAcceptsApprovedEmptySet(t *testing.T) {
	t.Parallel()
	if _, err := loadExclusions(""); err == nil {
		t.Fatal("missing exclusion path must fail")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "exclusions.json")
	writeJSON(t, path, exclusionFile{Schema: exclusionSchema, Approved: true, Reason: "verified empty set", RemoteKeyIDs: []int64{}})
	got, err := loadExclusions(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty approved exclusion set, got %d", len(got))
	}
}

func TestExecuteOptionsRequireExplicitGates(t *testing.T) {
	t.Parallel()
	base := runOptions{Execute: true, BatchSize: 250, Direction: "forward"}
	if err := validateOptions(base); err == nil {
		t.Fatal("execute without checksum/correlation/batch must fail")
	}
	base.ExpectedChecksum = strings.Repeat("a", 64)
	base.CorrelationID = "owner-relocation-test"
	base.BatchNumber = 1
	if err := validateOptions(base); err != nil {
		t.Fatal(err)
	}
	base.BatchSize = 251
	if err := validateOptions(base); err == nil {
		t.Fatal("oversized batch must fail")
	}
}

func TestExecuteRejectsUnsafeCorrelationID(t *testing.T) {
	t.Parallel()
	opts := runOptions{Execute: true, BatchSize: 1, Direction: "forward", ExpectedChecksum: strings.Repeat("a", 64), CorrelationID: "unsafe@example.com", BatchNumber: 1}
	if err := validateOptions(opts); err == nil {
		t.Fatal("unsafe correlation ID must be rejected")
	}
}

func TestExclusionsRejectTrailingJSON(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "exclusions.json")
	raw := `{"schema":"api-key-owner-relocation-exclusions/v1","approved":true,"reason":"test","remote_key_ids":[]} {}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadExclusions(path); err == nil {
		t.Fatal("trailing JSON must be rejected")
	}
}

func TestAuthSnapshotCacheKeyUsesFullSHA256(t *testing.T) {
	t.Parallel()
	if got := authSnapshotCacheKey("fixture"); got != "f16d05ec6b29248d2c61adb1e9263f78e4f7bace1b955014a2d17872cfe4064d" {
		t.Fatalf("unexpected cache key identifier: %s", got)
	}
}

func TestCompareHistoryRequiresExactFrozenCutoffTotals(t *testing.T) {
	t.Parallel()
	before := historySnapshot{UsageRows: 2, BillingRows: 2, TotalCost: 1.5, InputTokens: 10, OutputTokens: 4}
	if err := compareHistory(before, before, false); err != nil {
		t.Fatal(err)
	}
	after := before
	after.TotalCost++
	if err := compareHistory(before, after, false); err == nil {
		t.Fatal("history mutation must fail")
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func ptrInt64(value int64) *int64    { return &value }
func ptrString(value string) *string { return &value }

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
