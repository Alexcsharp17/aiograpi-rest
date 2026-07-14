# SS-panel Instagram Module

This fork is a first-party reference implementation of the SS-panel external
module contract. It is intentionally a REST microservice: SS-panel talks to
the facade, while the service owns Instagram sessions, proxies, provider
workers, account leases, and platform error classification.

## Local setup

The fork branch is always `sspanel-main`. Keep the original repository as the
`upstream` remote. From a recursive SS-panel checkout, the canonical contract
is next to this module:

```bash
python scripts/sync_sspanel_contract.py \
  --schema ../ss-toolkit/contracts/external-executor.schema.json
uv run pytest -q
uv run ruff check aiograpi_rest tests
```

## Boundary rules

- `POST /module/v1/jobs` accepts a versioned module operation, not a Prisma
  `Order` object.
- `idempotencyKey` is required for every start and workflow input request.
- `accountSelector` refers to opaque executor account IDs; the panel stores
  only a binding and public projection.
- Sessions, cookies, passwords, and proxy credentials remain in encrypted
  executor storage.
- Provider errors become typed external statuses and terminal challenge/login
  states stop retries.
- Progress events are bounded summaries. Full comment, DM, and credential
  payloads must never be written to logs or panel-facing results.

## Adding a capability

1. Add the action to the canonical schema and toolkit constants.
2. Regenerate `aiograpi_rest/sspanel_contract.json`.
3. Implement the facade validation, worker operation, and normalized events.
4. Add the capability to `instagramImplementedCapabilities` only when the
   operation is executable in the deployed module.
5. Add contract, idempotency, restart, policy, lease, error, and redaction
   tests before advertising it in the manifest.

The Instagram reference module currently advertises these direct capabilities:

- account health, profile lookup, comments list/reply/delete/pin;
- photo/video/reel/story upload;
- DM inbox/send/reply;
- basic account or media insights.

Media operations accept an HTTP(S) `mediaUrl`; the executor downloads it into
private temporary storage, enforces `SSPANEL_MEDIA_MAX_BYTES`, and removes it
after the provider call. Direct writes use one active account lease and a
durable provider-call marker so an executor restart never silently retries an
unknown write outcome.

The module does not import SS-panel business logic. Product pricing, user
entitlements, funnel prompts, and AI scenario resolution stay in the panel or
its content boundary.
