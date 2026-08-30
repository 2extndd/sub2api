package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

func loadManifest(path string) (*manifest, string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, "", errors.New("--manifest is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("read manifest: %w", err)
	}
	sum := sha256.Sum256(raw)
	checksum := hex.EncodeToString(sum[:])

	var value manifest
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&value); err != nil {
		return nil, "", fmt.Errorf("decode manifest: %w", err)
	}
	if err := requireJSONEOF(dec); err != nil {
		return nil, "", fmt.Errorf("decode manifest: %w", err)
	}
	if value.Schema != manifestSchema && value.Schema != subsetManifestSchema {
		return nil, "", fmt.Errorf("unsupported manifest schema %q", value.Schema)
	}
	if value.SensitiveValuesIncluded {
		return nil, "", errors.New("manifest declares sensitive values; refusing input")
	}
	if value.SourceProvider != "api2cn" || value.TargetProvider != "onesubprovider" {
		return nil, "", fmt.Errorf("unexpected provider pair %q -> %q", value.SourceProvider, value.TargetProvider)
	}
	if value.Schema == subsetManifestSchema {
		if value.EntryCount != requiredSubsetSize || len(value.Entries) != requiredSubsetSize {
			return nil, "", fmt.Errorf("subset manifest must contain exactly %d entries", requiredSubsetSize)
		}
		if strings.TrimSpace(value.MappingDigest) == "" {
			return nil, "", errors.New("subset manifest lacks mapping_digest")
		}
		value.Summary = manifestSummary{Total: len(value.Entries), Transfer: len(value.Entries)}
	}
	if err := validateManifestSummary(&value); err != nil {
		return nil, "", err
	}
	return &value, checksum, nil
}

func validateManifestSummary(value *manifest) error {
	if value == nil {
		return errors.New("manifest is nil")
	}
	counts := manifestSummary{Total: len(value.Entries)}
	remoteIDs := make(map[int64]struct{}, len(value.Entries))
	oneProviderIDs := make(map[int64]struct{}, len(value.Entries))
	fingerprints := make(map[string]struct{}, len(value.Entries))
	for i, entry := range value.Entries {
		switch entry.Action {
		case "correct":
			counts.Correct++
		case "transfer":
			counts.Transfer++
		case "absent":
			counts.Absent++
		default:
			return fmt.Errorf("manifest entry %d has unsupported action %q", i, entry.Action)
		}
		if entry.Excluded {
			counts.Excluded++
		}
		if entry.Action == "absent" {
			if entry.RemoteKeyID != nil || entry.CurrentUserID != nil {
				return fmt.Errorf("absent manifest entry %d contains remote ownership", i)
			}
			continue
		}
		if entry.RemoteKeyID == nil || *entry.RemoteKeyID <= 0 || entry.CurrentUserID == nil || *entry.CurrentUserID <= 0 {
			return fmt.Errorf("manifest entry %d lacks positive remote/current owner IDs", i)
		}
		if entry.ExpectedUserID <= 0 || entry.OneProviderKeyID <= 0 || entry.Fingerprint == nil || strings.TrimSpace(*entry.Fingerprint) == "" || entry.ProtectedDigest == nil || strings.TrimSpace(*entry.ProtectedDigest) == "" {
			return fmt.Errorf("manifest entry %d lacks required verification material", i)
		}
		if _, exists := oneProviderIDs[entry.OneProviderKeyID]; exists {
			return fmt.Errorf("duplicate oneprovider_key_id %d", entry.OneProviderKeyID)
		}
		oneProviderIDs[entry.OneProviderKeyID] = struct{}{}
		if _, exists := remoteIDs[*entry.RemoteKeyID]; exists {
			return fmt.Errorf("duplicate remote_key_id %d", *entry.RemoteKeyID)
		}
		remoteIDs[*entry.RemoteKeyID] = struct{}{}
		if _, exists := fingerprints[*entry.Fingerprint]; exists {
			return fmt.Errorf("duplicate fingerprint in manifest entry %d", i)
		}
		fingerprints[*entry.Fingerprint] = struct{}{}
	}
	if counts != value.Summary {
		return fmt.Errorf("manifest summary mismatch: calculated=%+v declared=%+v", counts, value.Summary)
	}
	return nil
}

func loadExclusions(path string) (map[int64]struct{}, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("--exclusions is required even when the approved set is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read exclusions: %w", err)
	}
	var value exclusionFile
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode exclusions: %w", err)
	}
	if err := requireJSONEOF(dec); err != nil {
		return nil, fmt.Errorf("decode exclusions: %w", err)
	}
	if value.Schema != exclusionSchema {
		return nil, fmt.Errorf("unsupported exclusions schema %q", value.Schema)
	}
	if !value.Approved {
		return nil, errors.New("exclusion gate is not approved")
	}
	if strings.TrimSpace(value.Reason) == "" {
		return nil, errors.New("approved exclusion file requires a reason")
	}
	result := make(map[int64]struct{}, len(value.RemoteKeyIDs))
	for _, id := range value.RemoteKeyIDs {
		if id <= 0 {
			return nil, fmt.Errorf("invalid excluded remote_key_id %d", id)
		}
		if _, exists := result[id]; exists {
			return nil, fmt.Errorf("duplicate excluded remote_key_id %d", id)
		}
		result[id] = struct{}{}
	}
	return result, nil
}

func requireJSONEOF(dec *json.Decoder) error {
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value is not allowed")
		}
		return err
	}
	return nil
}

func selectEntries(value *manifest, exclusions map[int64]struct{}, opts runOptions) ([]manifestEntry, runSummary, error) {
	if value == nil {
		return nil, runSummary{}, errors.New("manifest is nil")
	}
	summary := runSummary{
		ManifestTotal: value.Summary.Total,
		Correct:       value.Summary.Correct,
		Transfer:      value.Summary.Transfer,
		Absent:        value.Summary.Absent,
	}
	selected := make([]manifestEntry, 0, value.Summary.Transfer)
	for _, entry := range value.Entries {
		if entry.Action != "transfer" || entry.RemoteKeyID == nil {
			continue
		}
		_, externallyExcluded := exclusions[*entry.RemoteKeyID]
		if entry.Excluded || externallyExcluded {
			summary.Excluded++
			continue
		}
		if opts.OnlyID > 0 && *entry.RemoteKeyID != opts.OnlyID {
			continue
		}
		if opts.OnlyID == 0 && *entry.RemoteKeyID <= opts.AfterID {
			continue
		}
		selected = append(selected, entry)
	}
	sort.Slice(selected, func(i, j int) bool { return *selected[i].RemoteKeyID < *selected[j].RemoteKeyID })
	if opts.OnlyID > 0 && len(selected) != 1 {
		return nil, summary, fmt.Errorf("--only-id %d did not select exactly one transferable non-excluded row", opts.OnlyID)
	}
	if opts.Execute && len(selected) > opts.BatchSize {
		selected = selected[:opts.BatchSize]
	}
	summary.Selected = len(selected)
	summary.SelectedIDs = make([]int64, 0, len(selected))
	for _, entry := range selected {
		summary.SelectedIDs = append(summary.SelectedIDs, *entry.RemoteKeyID)
	}
	return selected, summary, nil
}
