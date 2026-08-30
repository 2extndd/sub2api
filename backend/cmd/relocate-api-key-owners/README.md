# relocate-api-key-owners

Internal one-shot maintenance CLI for guarded ownership relocation of existing `api_keys` rows.

It is not an HTTP endpoint and never recreates credentials. The command preserves the key row ID and all protected fields, changing only `user_id` and `updated_at` after manifest verification and row locking.

## Safety contract

- Dry-run by default.
- Requires the frozen v1 relocation manifest and an explicitly approved exclusion file.
- Execute mode requires the exact manifest SHA-256, a non-secret correlation ID, and a positive batch number.
- Maximum execute batch size is 250. Rollout uses a one-row canary, a 100-row validation batch, then serial batches up to 250.
- A canary is selected with `--only-id`.
- Forward and guarded rollback directions are supported.
- Every moved row writes a same-transaction append-only audit event.
- The installed database trigger durably enqueues auth-cache invalidation; the CLI also deletes Redis L2 and publishes L1 invalidation after commit.
- Usage and billing ledgers are immutable. The CLI compares a frozen-cutoff per-key history snapshot before and after relocation and fails if IDs/counts/amounts/tokens changed.
- Live quota/window counters can legitimately drift after manifest generation. `--allow-live-protected-drift` permits only the manifest mismatch; fingerprint/owner/target remain strict, and the current DB protected snapshot must remain identical through the locked update. Drift is counted and audited.
- Output contains only aggregate counts and a shortened manifest reference; it never prints credentials, fingerprints, protected digests, cache keys, payloads, or identity data.

## Exclusion file

```json
{
  "schema": "api-key-owner-relocation-exclusions/v1",
  "approved": true,
  "reason": "operator-approved protected and quarantined remote row IDs",
  "remote_key_ids": []
}
```

The file is required even when the approved set is empty. Missing or unapproved input fails closed.

## Full dry-run

```bash
relocate-api-key-owners \
  --manifest /run/relocation/manifest.json \
  --exclusions /run/relocation/exclusions.json
```

## One-row canary

```bash
relocate-api-key-owners \
  --manifest /run/relocation/manifest.json \
  --exclusions /run/relocation/exclusions.json \
  --manifest-sha256 "$EXPECTED_MANIFEST_SHA256" \
  --execute \
  --only-id "$CANARY_REMOTE_KEY_ID" \
  --batch-size 1 \
  --batch-number 1 \
  --correlation-id owner-relocation-20260827 \
  --allow-live-protected-drift
```

## One bounded batch

Use the largest committed remote row ID from the previous verified batch as `--after-id`.

```bash
relocate-api-key-owners \
  --manifest /run/relocation/manifest.json \
  --exclusions /run/relocation/exclusions.json \
  --manifest-sha256 "$EXPECTED_MANIFEST_SHA256" \
  --execute \
  --after-id "$LAST_VERIFIED_REMOTE_KEY_ID" \
  --batch-size 250 \
  --batch-number "$BATCH_NUMBER" \
  --correlation-id owner-relocation-20260827 \
  --allow-live-protected-drift
```

The command executes only one batch per invocation. Verify audit/cache/owner/protected-state results before invoking the next batch.

## Guarded rollback

Rollback uses the same manifest and exclusion gates, reverses expected/current owners, and creates new append-only audit events.

```bash
relocate-api-key-owners \
  --manifest /run/relocation/manifest.json \
  --exclusions /run/relocation/exclusions.json \
  --manifest-sha256 "$EXPECTED_MANIFEST_SHA256" \
  --execute \
  --direction rollback \
  --only-id "$REMOTE_KEY_ID" \
  --batch-size 1 \
  --batch-number "$ROLLBACK_BATCH_NUMBER" \
  --correlation-id owner-relocation-20260827-rollback
```
