# Lakehouse Seeding (demo/development only)

`scripts/seed.py` loads deterministic, idempotent, clearly-synthetic Nigerian
maritime demo events into the medallion lakehouse (Delta tables on local
object storage) and rebuilds the derived gold tables.

## Safety gates

The seeder **refuses** to run when `ENV=production` or `PROFILE=prod`, and
requires explicit acknowledgement via `SEED_DEMO=true`. It writes only to
`SEED_LAKE_ROOT` (default `./lakehouse-demo`); production lake roots are
never touched.

## Usage

```sh
SEED_DEMO=true SEED_LAKE_ROOT=./lakehouse-demo python scripts/seed.py
```

## What gets seeded

| Table | Layer | Rows |
|---|---|---|
| `cvff_bronze` | CVFF fiduciary-segregated bronze | 4 ledger-commitment envelopes |
| `cvff_silver` | CVFF silver (dedup on Kafka offset + ledger hash) | 4 |
| `cvff_gold` | one row per ledger commitment | 4 |
| `platform_bronze` | platform bronze | 4 stamps lifecycle envelopes |
| `platform_silver` | platform silver (excise gold input) | 4 |
| `platform_gold/excise_stamp_facts` | 1:1 fact projection of the stamps lifecycle | 4 |

All money is NGN kobo; identities (assessment ids, declaration refs, TINs)
are synthetic.

## Idempotency

Bronze/silver writes are insert-only merges deduplicated on `event_id`
(CVFF silver additionally on the composite dedup key); gold tables are
atomically rebuilt from silver. Proven by double-run: the second run writes
0 new rows (`present == 4` everywhere) — see
`db/seed/seed-coverage.json` (`second_run_noop`).
