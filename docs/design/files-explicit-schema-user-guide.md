# FILES() `schema` Parameter User Guide

## 1. What this feature solves

When reading multiple files with `FILES()`, automatic schema detection can be unstable if files evolve differently.
The `schema` parameter lets you declare what you want to read explicitly, so query behavior is more predictable.

This guide focuses on user behavior and query usage.

## 2. Where `schema` is supported

`schema` is supported in all read paths of `FILES()`:

- `SELECT ... FROM FILES(...)`
- `INSERT ... SELECT ... FROM FILES(...)`
- `CTAS ... AS SELECT ... FROM FILES(...)`

It is not used for unload/write (`INSERT INTO FILES`).

## 3. Quick start

Example:

```sql
SELECT user_id, event_time
FROM FILES(
    "path" = "s3://bucket/path/*.parquet",
    "format" = "parquet",
    "schema" = "user_id BIGINT, event_time DATETIME"
);
```

Each schema item must be `name type`.

## 4. Format behavior

### 4.1 Parquet / ORC / Avro

- Columns are matched by name.
- Matching is case-sensitive.
- If case does not match, the column is considered missing.

### 4.2 CSV

- Columns are matched by position.
- Schema column names are aliases only and can be arbitrary.
- Mapping is based on order:
  - first schema item -> first CSV column
  - second schema item -> second CSV column
  - and so on

## 5. Missing and extra columns

- Extra columns in files that are not declared in `schema` are ignored.
- Declared columns missing in some files follow existing `fill_mismatch_column_with` behavior:
  - `none`: fail
  - `null`: fill `NULL`

## 6. Parameter interaction rules

### 6.1 Mutual exclusion

Do not use `schema` together with:

- `auto_detect_sample_files`
- `auto_detect_sample_rows`
- `auto_detect_types`

Using them together is a validation error.

### 6.2 With `columns_from_path`

If `schema` and `columns_from_path` produce the same column name, query fails.

### 6.3 With `list_files_only`

If `list_files_only = true`, `schema` is silently ignored.

## 7. Supported and unsupported schema syntax

### Supported syntax

- `name type`
- complex types such as `ARRAY`, `MAP`, `STRUCT`
- partial nested declaration for complex types

Example:

```sql
"schema" = "request_data STRUCT<device_data STRUCT<platform VARCHAR(64)>, now BIGINT>"
```

### Not supported syntax

The following schema tokens are rejected:

- `NULL`
- `NOT NULL`
- `DEFAULT`

Example (invalid):

```sql
"schema" = "id BIGINT NOT NULL, dt DATE DEFAULT '2026-01-01'"
```

## 8. Recommended workflow before writing schema

Use `DESC FILES` first when:

- you do not know the source file schema yet,
- upstream files recently changed,
- same logical column may have different physical types across files.

Example:

```sql
DESC FILES(
    "path" = "s3://bucket/path/*.parquet",
    "format" = "parquet",
    "auto_detect_sample_files" = "1000"
);
```

If file diversity is high, increase `auto_detect_sample_files` so all target files are sampled, and the merged schema is representative enough to guide your explicit `schema`.

## 9. Common pitfalls

- **Case mismatch in Parquet/ORC/Avro**: `UserId` and `userid` are different.
- **Mixing `schema` with auto-detect parameters**: this always fails by design.
- **Expecting CSV name-based matching**: CSV uses position, not names.
- **Using column constraints inside `schema`**: `NOT NULL`/`DEFAULT` are not accepted.

