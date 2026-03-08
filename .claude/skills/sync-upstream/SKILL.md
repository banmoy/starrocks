---
name: sync-upstream
description: Sync a branch of origin with the corresponding upstream branch for this fork repository
disable-model-invocation: true
allowed-tools: Bash, Read, Grep, Glob, Agent
argument-hint: "[branch] [--dry-run]"
---

# Sync Fork with Upstream

Sync a branch of `origin` with the corresponding `upstream` branch by rebasing fork-specific commits on top of the latest upstream history, then force-pushing.

## Context

This repository is a fork of [StarRocks/starrocks](https://github.com/StarRocks/starrocks).
- Remote `upstream` = `https://github.com/StarRocks/starrocks.git`
- Remote `origin` = this fork

## Arguments

Parse `$ARGUMENTS` for the following:

- **branch**: The branch name to sync. Default: `main`. Examples: `main`, `branch-3.4`, `branch-4.0`.
- `--dry-run`: Only analyze and report, do not push changes.

Examples:
```
/sync-upstream                    # sync main (default)
/sync-upstream main               # sync main (explicit)
/sync-upstream branch-3.4         # sync branch-3.4
/sync-upstream main --dry-run     # analyze main only
/sync-upstream branch-4.0 --dry-run
```

Let `BRANCH` refer to the parsed branch name below.

## Procedure

Follow these steps **exactly in order**. Stop and report errors at any step.

### Step 1: Fetch and Analyze

```bash
git fetch origin $BRANCH
git fetch upstream $BRANCH
```

If either fetch fails (branch doesn't exist on that remote), report the error and stop.

Find the merge-base and analyze divergence:

```bash
MERGE_BASE=$(git merge-base origin/$BRANCH upstream/$BRANCH)
echo "Merge base: $MERGE_BASE"
git log --oneline $MERGE_BASE..upstream/$BRANCH | wc -l    # new upstream commits
git log --oneline $MERGE_BASE..origin/$BRANCH               # fork-specific commits
```

Report to the user:
- The branch being synced
- Number of new upstream commits
- List of fork-specific commits (with short SHAs and messages)
- The merge-base SHA

### Step 2: Create Sync Branch

Create a temporary branch from `upstream/$BRANCH`:

```bash
git checkout upstream/$BRANCH --detach
git checkout -b _sync-upstream-temp
```

### Step 3: Cherry-pick Fork-Specific Commits

Cherry-pick all fork-specific commits (from merge-base to origin/$BRANCH) onto the sync branch, **in order** (oldest first):

```bash
# Get the list of fork-specific commit SHAs, oldest first
FORK_COMMITS=$(git log --oneline --reverse $MERGE_BASE..origin/$BRANCH | awk '{print $1}')

# Cherry-pick each one
for sha in $FORK_COMMITS; do
  git cherry-pick $sha
done
```

If cherry-pick conflicts occur:
1. Report the conflicting commit and files to the user
2. Ask the user how to resolve
3. Do NOT skip commits without user approval

### Step 4: Verify

Before pushing, verify the result:

```bash
# Show the final commit log
git log --oneline upstream/$BRANCH..HEAD

# Verify fork-specific commits are on top
# Verify upstream history is intact
```

Report the final state to the user:
- Total commits on the branch
- Fork-specific commits on top
- Confirm upstream history matches `upstream/$BRANCH`

### Step 5: Push (skip if --dry-run)

If `$ARGUMENTS` contains `--dry-run`, stop here and report what would happen.

Otherwise, **ask the user for confirmation** before force-pushing:

```bash
git push origin _sync-upstream-temp:$BRANCH --force
```

After successful push:
```bash
# Clean up
git checkout $BRANCH
git branch -D _sync-upstream-temp
git pull origin $BRANCH
```

### Step 6: Report

Summarize:
- Which branch was synced
- How many upstream commits were synced
- Which fork-specific commits were preserved
- The new HEAD of `origin/$BRANCH`

## Important Notes

- **Why force push instead of PR?** Fork-specific commits exist on both `origin/$BRANCH` and the sync branch (with different SHAs after rebase). GitHub rebase merge would try to apply them twice, causing conflicts.
- **Always fetch fresh** before syncing to avoid stale state.
- **Never lose fork-specific commits** - they must all be preserved on top of upstream.
- Retry git network operations (fetch/push) up to 4 times with exponential backoff (2s, 4s, 8s, 16s) on failure.
