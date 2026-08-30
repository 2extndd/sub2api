package main

import (
	"database/sql"
	"encoding/json"
	"time"
)

const (
	manifestSchema       = "onesub-owner-relocation-manifest/v1"
	subsetManifestSchema = "onesub-owner-relocation-subset/v1"
	exclusionSchema      = "api-key-owner-relocation-exclusions/v1"
	maxBatchSize         = 250
	requiredSubsetSize   = 8
)

type manifest struct {
	Schema                  string          `json:"schema"`
	GeneratedAt             string          `json:"generated_at"`
	SensitiveValuesIncluded bool            `json:"sensitive_values_included"`
	SourceProvider          string          `json:"source_provider"`
	TargetProvider          string          `json:"target_provider"`
	Summary                 manifestSummary `json:"summary,omitempty"`
	EntryCount              int             `json:"entry_count,omitempty"`
	MappingDigest           string          `json:"mapping_digest,omitempty"`
	Entries                 []manifestEntry `json:"entries"`
}

type manifestSummary struct {
	Total    int `json:"total"`
	Correct  int `json:"correct"`
	Transfer int `json:"transfer"`
	Absent   int `json:"absent"`
	Excluded int `json:"excluded"`
}

type manifestEntry struct {
	Action                  string  `json:"action"`
	CoefficientDigest       *string `json:"coefficient_digest"`
	CurrentReplicaCabinetID *int64  `json:"current_replica_cabinet_id"`
	CurrentUserID           *int64  `json:"current_user_id"`
	ExpectedUserID          int64   `json:"expected_user_id"`
	Excluded                bool    `json:"excluded"`
	ExclusionReason         *string `json:"exclusion_reason"`
	Fingerprint             *string `json:"fingerprint"`
	OneProviderKeyID        int64   `json:"oneprovider_key_id"`
	ProtectedDigest         *string `json:"protected_digest"`
	RemoteKeyID             *int64  `json:"remote_key_id"`
	SourceCabinetID         int64   `json:"source_cabinet_id"`
	TargetCabinetID         int64   `json:"target_cabinet_id"`
}

type exclusionFile struct {
	Schema       string  `json:"schema"`
	Approved     bool    `json:"approved"`
	Reason       string  `json:"reason"`
	RemoteKeyIDs []int64 `json:"remote_key_ids"`
}

type keyRow struct {
	ID            int64
	UserID        int64
	Key           string
	Quota         float64
	QuotaUsed     float64
	ExpiresAt     sql.NullTime
	GroupID       sql.NullInt64
	Status        string
	IPBlacklist   json.RawMessage
	IPWhitelist   json.RawMessage
	RateLimit5h   float64
	RateLimit1d   float64
	RateLimit7d   float64
	Usage5h       float64
	Usage1d       float64
	Usage7d       float64
	Window5hStart sql.NullTime
	Window1dStart sql.NullTime
	Window7dStart sql.NullTime
	CreatedAt     time.Time
	UpdatedAt     time.Time
	DeletedAt     sql.NullTime
}

type historySnapshot struct {
	UsageRows        int64
	BillingRows      int64
	BillingApplied   int64
	BillingDeltaUSD  float64
	TotalCost        float64
	InputTokens      int64
	OutputTokens     int64
	FirstUsage       sql.NullTime
	LastUsage        sql.NullTime
	DistinctUserRows int64
}

type validatedCandidate struct {
	Entry                  manifestEntry
	Row                    keyRow
	ProtectedDigest        string
	Fingerprint            string
	History                historySnapshot
	HistoryCutoff          time.Time
	ManifestProtectedMatch bool
}

type runOptions struct {
	ManifestPath        string
	ExclusionsPath      string
	ExpectedChecksum    string
	Execute             bool
	BatchSize           int
	AfterID             int64
	OnlyID              int64
	CorrelationID       string
	BatchNumber         int
	Direction           string
	AllowProtectedDrift bool
	RepairCacheOnly     bool
}

type runSummary struct {
	ManifestTotal  int
	Correct        int
	Transfer       int
	Absent         int
	Excluded       int
	Selected       int
	Validated      int
	Moved          int
	CacheCleared   int
	HistoryRows    int64
	BillingRows    int64
	ProtectedDrift int
	SelectedIDs    []int64
}
