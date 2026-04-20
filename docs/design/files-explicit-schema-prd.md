# FILES() Explicit Schema Parameter PRD

## 1. Background and Goal

`FILES()` currently relies on schema inference and merge across input files. In multi-file scenarios with schema drift, especially for nested types, inference results can be unstable and hard for users to control. This PRD defines a new `schema` parameter for `FILES()` so users can explicitly declare the read schema and get deterministic behavior.

This design keeps behavior aligned with existing `FILES()` semantics as much as possible. The new parameter should provide control, not introduce a new conversion model.

## 2. Scope

### In Scope

- All read paths of `FILES()`:
  - `SELECT ... FROM FILES(...)`
  - `INSERT ... SELECT ... FROM FILES(...)`
  - `CTAS ... AS SELECT ... FROM FILES(...)`
- All read formats:
  - `parquet`
  - `orc`
  - `avro`
  - `csv`

### Out of Scope

- `DESC FILES` with `schema` parameter support.
- `INSERT INTO FILES` unload/write path.
- New constraint syntax in `schema` (for example `NULL`, `NOT NULL`, `DEFAULT`).

## 3. Product Definition

### 3.1 New Parameter

- `schema`: explicit schema declaration string for `FILES()` reads.
- Syntax: comma-separated `column_name TYPE` items.
- Supported type grammar follows StarRocks type syntax, including complex types (`ARRAY`, `MAP`, `STRUCT`).

### 3.2 Behavior Contract

When `schema` is provided:

- Read columns are determined by `schema`.
- Extra columns existing in files but not declared in `schema` are ignored.
- Missing declared columns follow existing `fill_mismatch_column_with` behavior:
  - `none`: fail
  - `null`: fill with `NULL`

### 3.3 Non-supported Syntax

The following must be rejected as invalid `schema` syntax:

- `NULL`
- `NOT NULL`
- `DEFAULT ...`

Only `name type` is supported for each declared column.

## 4. Format-specific Matching Rules

### 4.1 Parquet / ORC / Avro

- Matching is by column name.
- Column name matching is case-sensitive (aligned with current behavior).
- Case mismatch is treated as "column not matched", then follows missing-column behavior via `fill_mismatch_column_with`.

### 4.2 CSV

- Matching is by position, not by column name.
- The first schema field maps to file column 1, second maps to file column 2, etc.
- Schema column names are logical aliases and can be arbitrary in CSV mode.

Example (equivalent mapping for CSV):

- `"schema" = "a BIGINT, b VARCHAR(64)"`
- `"schema" = "user_id BIGINT, comment VARCHAR(64)"`

### 4.3 Complex Types

- Partial complex type declaration is supported.
- For `STRUCT`, users may declare only required child fields.
- Undeclared nested fields are ignored.

## 5. Parameter Interaction Rules

### 5.1 Mutual Exclusion

`schema` is mutually exclusive with:

- `auto_detect_sample_files`
- `auto_detect_sample_rows`
- `auto_detect_types`

If specified together, query must fail at validation stage.

### 5.2 With `columns_from_path`

- If `schema` and `columns_from_path` produce the same column name, fail directly (aligned with current behavior).

### 5.3 With `fill_mismatch_column_with`

- Missing-column behavior under explicit schema fully follows existing `fill_mismatch_column_with` semantics.

### 5.4 With `list_files_only`

- If `list_files_only = true`, `schema` is silently ignored.

## 6. Error Semantics

Error handling follows three layers.

### 6.1 Parameter/Syntax Errors (fail early)

- `schema` with any `auto_detect_*` parameter.
- `schema` with duplicate name conflict against `columns_from_path`.
- Invalid schema grammar.
- Use of unsupported tokens (`NULL`, `NOT NULL`, `DEFAULT`).

### 6.2 Mapping-level Errors

- Name mismatch (including case mismatch) for `parquet/orc/avro` leads to missing-column path.
- Missing-column final behavior is controlled by `fill_mismatch_column_with`.

### 6.3 Conversion-level Errors

Keep current `FILES()` behavior for type incompatibility, overflow, parse failures, and value conversion:

- `strict_mode = false`: preserve current best-effort conversion path (including nullification where applicable).
- `strict_mode = true`: preserve current strict filtering/rejection behavior.

No new conversion exception policy is introduced.

## 7. User Guidance Before Writing `schema`

Users often do not know the exact file schema before writing explicit schema. Recommend a pre-check workflow:

1. Run `DESC FILES(...)` on the target file set.
2. Use merged schema output as baseline guidance for explicit schema drafting.
3. If source files may have schema drift, increase `auto_detect_sample_files` to cover all target files, so merge guidance is representative.

Recommended scenarios for this workflow:

- Initial onboarding with unknown file schema.
- Upstream schema changed and impact is unclear.
- Same logical column may use different physical types across files and promotion needs to be decided.

This PRD intentionally does not include a static type-conflict cheat sheet because it is hard to maintain and not user-friendly.

## 8. Examples

### 8.1 Parquet by name

```sql
SELECT user_id, event_time
FROM FILES(
  "path" = "s3://bucket/path/*.parquet",
  "format" = "parquet",
  "schema" = "user_id BIGINT, event_time DATETIME"
);
```

### 8.2 CSV by position

```sql
SELECT *
FROM FILES(
  "path" = "s3://bucket/path/*.csv",
  "format" = "csv",
  "csv.column_separator" = ",",
  "schema" = "a BIGINT, b VARCHAR(64)"
);
```

### 8.3 Partial nested declaration

```sql
SELECT request_data.device_data.platform, request_data.now
FROM FILES(
  "path" = "s3://bucket/path/*.parquet",
  "format" = "parquet",
  "schema" = "request_data STRUCT<device_data STRUCT<platform VARCHAR(64)>, now BIGINT>"
);
```

### 8.4 Mutual exclusion error

```sql
SELECT *
FROM FILES(
  "path" = "s3://bucket/path/*.parquet",
  "format" = "parquet",
  "schema" = "user_id BIGINT",
  "auto_detect_sample_files" = "2"
);
```

Expected: validation failure due to mutual exclusion.

## 9. Compatibility and Release

- This is an additive optional parameter, not a breaking change.
- Existing queries without `schema` must keep current behavior.
- Release notes/docs should explicitly call out:
  - `schema` vs `auto_detect_*` mutual exclusion
  - case-sensitive name matching for `parquet/orc/avro`
  - positional mapping for `csv`
  - missing-column handling still controlled by `fill_mismatch_column_with`

## 10. Acceptance Criteria

### Functional

- `schema` is available for all read paths of `FILES()`.
- `schema` is rejected in unsupported syntax forms (`NULL/NOT NULL/DEFAULT`).
- `schema` is ignored when `list_files_only = true`.

### Interaction

- Mutual exclusion checks with `auto_detect_*` are enforced.
- Conflict check with `columns_from_path` is enforced.
- Missing-column behavior matches existing `fill_mismatch_column_with`.

### Format Behavior

- `parquet/orc/avro` name-based and case-sensitive matching works as defined.
- `csv` positional mapping works as defined, independent from schema column names.
- Partial complex-type declaration works as defined.

### Consistency

- Strict/non-strict conversion behavior remains aligned with current `FILES()` implementation.

