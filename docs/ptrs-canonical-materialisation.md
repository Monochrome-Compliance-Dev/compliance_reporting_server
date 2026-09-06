# PTRS canonical materialisation checkpoint

## Execution and ownership

Canonical materialisation remains a synchronous PTRS operation, separate from Stage.
The unique key `(customerId, ptrsId, datasetId, materialSignature)` applies to
`building` and `succeeded` revisions. Inserting and committing `building` is the
execution claim: only the inserting request enters preparation/composition. Other
requests return the existing revision (`202` building, `200` succeeded) with the
existing `{ revision, reused }` data shape. Unique-insert races are resolved in a
fresh transaction. Failed attempts remain historical and do not hold the key.

There is no session advisory lock, process-global cache, automatic job restart or
age-based ownership takeover. The owner locks its revision row during the build;
ordinary readers can still observe the committed `building` claim. Setup, build
and failure recording each use a customer-scoped transaction. Only the build
transaction writes canonical rows, and it publishes all rows and `succeeded`
together. A committed claim alone is never usable Stage input.

The unique index is required before running the new application version. Drain
old application instances before migration; mixed old/new writers are unsupported.
The application model also declares the index, so do not restart model sync before
reconciling old orphan records and applying the migration.

## Prepared input and source consistency

Setup uses repeatable-read to capture one configuration and material identity.
The requested profile, dataset-owned field mappings, reachable join graph and
custom fields are copied and frozen. Both hashing and execution use these values.
Traversal excludes other transaction datasets. Mapping material now includes
`datasetId`; mapping order has an explicit dataset tie-breaker. Join/custom-field
order is preserved in the hash because execution order can matter.

The build uses a separate repeatable-read transaction. Before index preparation,
it rechecks dataset metadata, raw counts and raw maximum update timestamps against
setup. A mismatch produces `409 CANONICAL_INPUT_CHANGED` and a failed claim; no
automatic replay occurs. Matching sources are read from that build snapshot for
the entire execution. Configuration changes after setup do not alter the frozen
execution; later current-revision resolution detects the new configuration.

`prepareMappedRowsContext()` normalises once and builds execution-local supporting
row/index caches. `composeMappedRowsForPtrs()` reuses that context for bounded
2,000-row primary batches, rejecting cross-tenant/dataset/transaction reuse.
Non-canonical preview callers prepare their own context per preview invocation.
Reference reads keep `ORDER BY rowNo ASC` and first-normalised-key-wins behaviour.
Custom fields, date policy and source/join lineage remain in the shared composer.

For 309,280 primary rows and 524,686 invoice rows, a complete build changes from
155 full invoice loads (81,326,330 logical invoice rows) to one full invoice load
(524,686 rows). This is structural read-count evidence, not a measured RP10 runtime.
There are still metadata/count checks at setup and before building.

Canonical identity is now `ptrs-canonical-v3`. Existing successful v2 rows remain
immutable history; their identities are not relabelled or silently reused under
the corrected input contract. Rebuild canonical and then Stage for current output.

Remaining limitations:

- Raw identity remains metadata/count/timestamp based, not a content revision.
  Changes that deliberately preserve those values are not reliably detected.
- Supporting datasets/indexes are still loaded into Node memory once per build.
  Measure peak memory before assuming the RP10 shape fits production limits.
- The build transaction remains long-lived, intentionally preserving atomicity.
- A hard process death or database outage can leave a committed `building` claim.
  There is no background worker to reconcile it. Operator-confirmed recovery is
  required; elapsed age is never proof that an owner is dead.
- If configuration changes during Map's sequence, Stage's current-revision check
  can reject the old output; Stage must not repair it by materialising implicitly.

## Failure handling

Rollback is attempted and any rollback error is logged separately. Failure status
is then attempted in a new customer transaction, even if rollback threw or build
connection acquisition failed. The original materialisation error is surfaced.
Sequelize destroys an uncertain connection when rollback/commit fails; it is not
reused for failure recording. Failure recording only changes `building`, so an
ambiguous commit acknowledgement cannot overwrite a successful publication.
If failure recording also fails, the log explicitly requires operator recovery.

The frontend opts out of generic POST retries for this endpoint only. Its API
normalises `ready`; Map retains its busy/ref guards, builds datasets sequentially,
and does not update the step or navigate unless all revisions are ready. A
contender stays on Map with an informational alert; it may explicitly retry later.
No new automatic polling or implicit Stage materialisation is introduced.

## Migration and known development orphan recovery

Apply `db/migrations/20260831_ptrs_canonical_active_material.sql` with the migration
administrator. It refuses any unresolved `building` row and replaces the
succeeded-only index with the active-material unique index. It does not change or
delete revision data. `row_security = off` prevents an accidentally tenant-limited
preflight; the administrator must have appropriate RLS-bypass privileges.

Before the following development-only SQL, stop/drain all canonical request owners
and verify the two recorded attempts are abandoned. An open browser request or an
old timestamp is not sufficient evidence. In particular, old code did not hold
the new revision-row lock throughout its build. Do not run this against a live
owner. This SQL was not executed against the development database in this task.

```sql
BEGIN;
SET LOCAL app.current_customer_id = '8Y1o0gWPoK';
SET LOCAL lock_timeout = '3s';

SELECT "id", "status", "materialSignature"
FROM "public"."tbl_ptrs_canonical_revision"
WHERE "customerId" = '8Y1o0gWPoK'
  AND "ptrsId" = 'tLIURXt66M'
  AND "datasetId" = '5OiEYXPR4o'
  AND "id" IN ('4EFSLxoxi9', 'UTP5DjvxoB')
FOR UPDATE NOWAIT;

UPDATE "public"."tbl_ptrs_canonical_revision"
SET "status" = 'failed',
    "completedAt" = clock_timestamp(),
    "failure" = jsonb_build_object(
      'code', 'MANUALLY_CONFIRMED_ABANDONED',
      'message', 'RP10 canonical backend terminated; operator verified no live owner'
    )
WHERE "customerId" = '8Y1o0gWPoK'
  AND "ptrsId" = 'tLIURXt66M'
  AND "datasetId" = '5OiEYXPR4o'
  AND "id" IN ('4EFSLxoxi9', 'UTP5DjvxoB')
  AND "status" = 'building'
RETURNING "id", "status", "failure";

COMMIT;
```

Expect two updated rows for the original unreconciled state. If the scope/statuses
differ, investigate rather than broadening the UPDATE. No historical data is
deleted. Review other `building` rows individually if the migration still refuses.
Do not rerun the destructive original canonical-table migration.

## Diagnostic events and clean RP10 rerun

`PTRS canonical lifecycle` logs carry operation/request IDs, scope, material
signature, revision ID and build backend PID when available. Events distinguish
start, input preparation, ownership/reuse/contention, context preparation,
supporting loads, index timing/cardinality, batch ranges/counts, compose/persistence
timing and completion/failure. RSS/heap are sampled at start/context/completion.
Reference loads log one load and row count per supporting dataset. Canonical
execution disables raw-value join probes; it does not log invoice rows.

1. Drain old writers; verify no orphan backends survive. Run the scoped development
   recovery above, apply the new migration, then start the updated backend/frontend.
2. Retain the original failed run as contaminated evidence, not a Stage baseline.
   Prevent unrelated canonical/Stage/Transformation work during measurement.
3. Record the pre-canonical database temp counters. In Map, uncheck **Auto-run
   stage**, then select **Next: Stage data** once. The
   new v3 signature builds fresh canonical output without changing source files.
4. Record canonical start/complete events separately for each transaction dataset.
   For the primary dataset expect 155 output batches, 309,280 rows, and exactly one
   invoice supporting-load event containing 524,686 rows. No second operation should
   reach context preparation for the same active signature.
5. Check canonical status: both transaction datasets must have current succeeded
   revisions. If a build is contended, remain on Map; investigate its owner/status.
6. Record the database temp-counter baseline immediately before canonical and its
   post-canonical values. Record a fresh baseline before Stage. Treat these as
   database-wide deltas, not statement attribution; unavailable readings are null.
   Capture PID-attributed temp logs/plans separately if spill attribution is needed.
7. On Stage, explicitly run persistence once. Measure its existing `stage_begin`
   and completion/timing evidence separately. Confirm it reads canonical revisions
   and does not emit canonical preparation events.
8. Record elapsed time, peak Node memory and row counts; distinguish warm reuse
   from fresh canonical work. A second unchanged Map Continue should reuse both
   successful revisions and perform no reference-index preparation.

No work_mem, Stage indexes, Transformation/SBI/metrics or timestamp-display changes
belong to this checkpoint. Brisbane console display remains a separate cleanup.

## Focused validation

Backend unit/semantic suites (no default integration setup):

```sh
npm run test:unit -- --runInBand --runTestsByPath \
  v2/ptrs/services/canonical.ptrs.service.test.js \
  v2/ptrs/services/maps.custom-field-joins.ptrs.service.test.js \
  v2/ptrs/services/maps.dataset-scope.ptrs.service.test.js \
  v2/ptrs/services/canonical.date-policy.ptrs.service.test.js \
  v2/ptrs/services/stage.build.ptrs.service.test.js \
  v2/ptrs/controllers/canonical.ptrs.controller.test.js
```

The opt-in `canonical.ownership.pg.test.js` requires a disposable, loopback
PostgreSQL database named exactly `canonical_materialisation_test` via
`CANONICAL_PG_TEST_URL`. It resets only its two fixture tables. It runs the actual
migration, real service claims/row locks/publication, and termination recovery;
source metadata/composition dependencies are fixtures. It does not use the
application database or the unrelated default integration-test setup.

```sh
CANONICAL_PG_TEST_URL=postgres://TEST_USER@127.0.0.1:TEST_PORT/canonical_materialisation_test \
  npm run test:unit -- --runInBand --runTestsByPath \
  v2/ptrs/services/canonical.ownership.pg.test.js
```

Frontend focused tests:

```sh
CI=true npm test -- --watchAll=false --runInBand --runTestsByPath \
  src/slices/ptrs/services/maps.canonical.ptrsApi.test.js \
  src/slices/ptrs/services/maps.canonical.retry.test.js \
  src/slices/ptrs/panels/MapPanel.canonical.test.js
```
