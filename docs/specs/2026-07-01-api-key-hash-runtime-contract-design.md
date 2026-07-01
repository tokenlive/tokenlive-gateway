# API Key Hash Runtime Contract Design

## Goal

Unify TokenLive Admin and Gateway API key runtime lookup around a hash-based Redis contract while preserving the existing product ability to show the full plaintext API key to an authorized user.

## Current State

- `tokenlive-admin` stores user and tenant API keys in database `api_key` fields and exposes `/api/v1/user-api-keys/{id}/plaintext` for authorized plaintext viewing.
- `tokenlive-admin` previously synced runtime auth data to Redis under `aigw:apikey:<plaintext>`.
- `tokenlive-gateway` previously validated Redis-backed API keys from `aigw:apikey:<plaintext>`.
- The runtime contract now removes plaintext Redis lookup and uses `aigw:apikey_hash:<hash>` only.

## Contract

Runtime validation uses:

```text
key_hash = HMAC-SHA256(api_key, GATEWAY_API_KEY_PEPPER)
redis_key = aigw:apikey_hash:<key_hash>
```

The Redis hash stores only runtime metadata:

```text
user_id
tenant
workspace_id
user_tenant
status
quota
expires_at
```

Redis must not use the plaintext API key as a runtime lookup key.

## Plaintext Display

Plaintext display remains an Admin/Portal concern. Gateway must not need database plaintext, ciphertext, or decryption capability.

For this phase, keep the existing database plaintext field so `/plaintext` behavior does not regress. A later phase can replace that field with encrypted storage without changing the gateway runtime contract.

## Migration Strategy

1. Admin writes `aigw:apikey_hash:<hash>` for user and tenant API keys.
2. Admin delete/update/sync paths target `aigw:apikey_hash:<hash>` only.
3. Gateway requires `llm.api_key_pepper` for Redis-backed API key lookup.
4. Gateway validation and quota deduction use the same resolved hash Redis key.
5. Existing `aigw:apikey:<plaintext>` keys should be removed through an operational cleanup after all services deploy the hash contract.

## Non-Goals

- Do not remove the Admin plaintext viewing API in this phase.
- Do not introduce key encryption/KMS in this phase.
- Do not change upstream provider API key storage.
- Do not change endpoint/provider routing config semantics.

## Verification

- Gateway unit tests must cover hash lookup, missing pepper rejection, legacy plaintext key rejection, and quota deduction on hash records.
- Admin tests should cover user API key sync/delete and full Redis resync writing hash keys.
- Existing plaintext display behavior should remain unchanged.
