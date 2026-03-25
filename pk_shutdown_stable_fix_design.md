# PK shutdown stale-entry stable fix

## Background

This fix targets a regression in the PK tablet migration path under the sequence
`A -> B -> A`.

The problematic flow is:

1. Tablet `V1` lives on disk `A`.
2. Migration `A -> B` succeeds. `V1` is moved into `_shutdown_tablets`, and the
   new tablet `V2` becomes active on disk `B`.
3. GC sweeps `V1`'s on-disk metadata on disk `A`, but a stale shutdown entry for
   `V1` can still remain in `_shutdown_tablets`.
4. Migration `B -> A` later creates `V3` on disk `A`.
5. `_add_shutdown_tablet_unlocked()` sees the old shutdown entry for the same
   `tablet_id` and unconditionally calls `_remove_tablet_meta()` on that stale
   entry.
6. For PK tablets, `TabletUpdates::clear_meta()` performs destructive cleanup by
   `tablet_id`, so the stale `V1` cleanup can wipe `V3`'s rowset metadata.

The bug is not that GC runs too late, nor that migration needs more global
coordination. The immediate corruption comes from the stale shutdown entry doing
destructive cleanup after ownership has already changed.

## Design goals

The user requirements for this fix are:

- Prefer stability over broad cleanup.
- Fix the local corruption point instead of changing more migration behavior.
- Avoid introducing new exception paths or additional failure handling.
- Keep behavior on unrelated paths as unchanged as possible.
- Keep the fix valid for PK tablets while not creating surprising behavior for
  non-PK tablets.

## Chosen fix

Apply a stale-entry guard directly inside
`be/src/storage/tablet_manager.cpp` in
`TabletManager::_add_shutdown_tablet_unlocked()`.

When a previous shutdown entry with the same `tablet_id` already exists:

1. Re-read on-disk `TabletMeta` from the old shutdown tablet's original
   `data_dir`.
2. If the meta still exists and either:
   - `tablet_uid` no longer matches the shutdown entry, or
   - `tablet_state` is no longer `TABLET_SHUTDOWN`,
   then treat that shutdown entry as stale.
3. For a stale entry, skip `_remove_tablet_meta()` in this duplicate-entry path.
4. Keep the rest of the existing queue flow unchanged:
   - move the old entry from `_shutdown_tablets` to
     `_shutdown_tablets_redundant_map`
   - insert the new `drop_info` into `_shutdown_tablets`

This keeps the fix narrowly focused on the destructive clear that is unsafe
after ownership has already changed.

## Why this is safe

### 1. It re-checks ownership at the exact destructive point

The critical operation is `_remove_tablet_meta()`. Re-reading meta immediately
before that call keeps the validation colocated with the destructive action.

This avoids expanding migration preflight or trying to reason about ownership in
an earlier phase where state may change again before cleanup actually happens.

### 2. It keeps the normal retirement path

Skipping `_remove_tablet_meta()` here does not skip retirement of the old
shutdown entry itself.

The old entry is still:

- removed from `_shutdown_tablets`
- moved into `_shutdown_tablets_redundant_map`
- left for the existing shutdown sweep flow to retire

So the behavior change is intentionally narrow: only the premature destructive
cleanup is skipped for entries that are already stale.

### 3. It does not add new error handling semantics

The fix does not make migration fail, retry, or branch into a new recovery path.
It only avoids a destructive delete when on-disk ownership clearly no longer
matches the stale shutdown entry.

## Why the guard only triggers on explicit stale evidence

The guard intentionally triggers only when `TabletMetaManager::get_tablet_meta()`
returns `ok()` and the meta proves that the shutdown entry is stale:

- `tablet_uid` mismatch
- or `tablet_state != TABLET_SHUTDOWN`

It does **not** treat `meta not found` as stale evidence in this path.

Reason:

- `not found` is ambiguous. It may mean GC already removed the old meta, not that
  a new tablet has taken ownership on the same disk.
- Extending the skip to `not found` would change existing retirement behavior
  more broadly and may leave more entries depending on later sweep ordering.
- The corruption case we care about is specifically "new owner already exists",
  which is positively identified by `ok() + uid/state mismatch`.

This keeps the change minimal and evidence-based.

## Why this applies to non-PK tablets too

The stale-entry guard is based on ownership semantics, not PK-specific storage
format.

If a shutdown entry's on-disk meta now belongs to a different tablet instance,
that entry is stale regardless of table type. Skipping destructive cleanup in
that case is the correct ownership-preserving behavior for both PK and non-PK
tables.

The main user-visible bug is more severe on PK tablets because PK cleanup can
clear rowset meta by `tablet_id`, but applying the stale-entry ownership guard
uniformly keeps the logic simpler and avoids type-specific cleanup rules in this
path.

## Why not fix this in migration preflight

An alternative considered was expanding migration preflight, for example by
forcing `delete_shutdown_tablet()` or similar cleanup before migration proceeds.

That approach was rejected because it:

- changes more of the migration path than necessary
- affects unrelated migrations that do not hit this stale-entry case
- creates more opportunity for behavior changes outside the exact corruption
  point

The chosen fix stays at the narrowest point where the bug is caused.

## Residual behavior and cleanup

If `_add_shutdown_tablet_unlocked()` skips destructive cleanup for a stale
entry, that entry can still be retired later by the normal shutdown sweeping
logic.

This is acceptable because:

- the stale entry no longer owns the current on-disk meta
- immediate destructive cleanup is the unsafe part
- queue retirement already exists and is the intended place to finish old
  shutdown entries

The fix therefore prefers correctness of current ownership over eagerness of
cleanup.

## Code changes

### `be/src/storage/tablet_manager.cpp`

- add a failpoint include
- add a failpoint in `start_trash_sweep()` after phase-1 sweep work and before
  finished shutdown entries are erased from the in-memory queues
- add the stale-entry meta re-check in `_add_shutdown_tablet_unlocked()`
- define the failpoint at file scope

### `be/test/storage/task/engine_storage_migration_task_test.cpp`

- add failpoint and `TabletMetaManager` includes
- add a regression test covering:
  - PK tablet creation with committed rowset meta
  - migration `A -> B`
  - GC sweep that removes old on-disk meta but intentionally leaves the stale
    shutdown entry in `_shutdown_tablets`
  - migration `B -> A`
  - rowset meta preservation on disk `A`
  - restart-like reload through `load_tablet_from_meta()` using the normal load
    path

## Test strategy

The regression test should verify the fixed behavior directly:

1. Create a PK tablet and commit at least one rowset.
2. Migrate `A -> B`.
3. Run `start_trash_sweep()` with a failpoint that leaves the stale shutdown
   entry in memory after phase-1 cleanup.
4. Confirm the old on-disk meta on `A` is already gone.
5. Migrate `B -> A`.
6. Assert rowset metadata still exists on `A`.
7. Drop only the in-memory tablet with `kKeepMetaAndFiles`.
8. Reload the tablet through `load_tablet_from_meta()` and verify the expected
   PK update state is preserved.

This reproduces the ownership race deterministically while keeping the production
change itself minimal.
