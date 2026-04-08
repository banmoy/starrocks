# Proposed Solution: Explicit Schema Specification in FILES

## Background

When reading multiple Parquet files with `FILES()`, schema inference may produce unexpected results when files have inconsistent nested structures. Currently, the system merges schemas only at the top level—if nested structs differ across files, the entire struct may be demoted to `VARCHAR`, causing nested field access to fail.

## Solution

Explicitly specify the schema for the columns you need using the `schema` parameter in `FILES()`, bypassing automatic schema inference.

## What the `schema` Parameter Does

The `schema` parameter declares the columns you want to read and their StarRocks data types. It uses the same syntax as a StarRocks `CREATE TABLE` column definition.

### Behavior

**Without `schema`**: StarRocks samples a subset of files, infers column names/types from Parquet metadata, and attempts to merge schemas. This can cause issues:

- **Missing columns**: If a column only exists in files that weren't sampled, it won't appear in the inferred schema.  
- **Nested struct merge failures**: If nested structs differ across sampled files, the entire struct may be demoted to `VARCHAR`, making nested fields inaccessible.

**With `schema`**: StarRocks skips inference entirely. It reads only the columns you declare and converts them to the specified StarRocks types.

### Syntax

```
"schema" = "column_name TYPE, column_name TYPE, ..."
```

The types are standard StarRocks data types (same as `CREATE TABLE`):

- `BIGINT`, `INT`, `SMALLINT`, `TINYINT`  
- `DOUBLE`, `FLOAT`, `DECIMAL(p, s)`  
- `VARCHAR(n)`, `CHAR(n)`, `STRING`  
- `BOOLEAN`  
- `DATE`, `DATETIME`  
- `STRUCT<...>`, `ARRAY<...>`, `MAP<...>`

**For nested fields**, use the standard StarRocks syntax:

```sql
"schema" = "request_data STRUCT<device_data STRUCT<platform VARCHAR(64)>, now BIGINT>"
```

---

## User Journey

### Step 1: Identify the fields you need

Determine which Parquet fields your query requires. You can explore the schema using `DESC FILES` on a single file:

```sql
DESC FILES(
    "path" = "gs://your-bucket/path/sample.parquet",
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "format" = "parquet"
);
```

### Step 2: Map Parquet types to StarRocks types

Choose a type that can accommodate all variations across files. For example, if some files have `INT` and others have `BIGINT`, use `BIGINT`. If types are incompatible (e.g., `INT` vs `STRING`), use `VARCHAR` as a catch-all.

**Example: Extracting only specific fields from a nested struct**

If your Parquet file has a deeply nested structure like:

```
request_data STRUCT<
    device_data STRUCT<
        platform VARCHAR,
        os_version VARCHAR,
        device_id VARCHAR,
        manufacturer VARCHAR
    >,
    user_data STRUCT<
        user_id BIGINT,
        session_id VARCHAR,
        preferences STRUCT<...>
    >,
    now BIGINT
>
```

But you only need `platform` from `device_data` and `now`, you can declare a partial struct that includes only the fields you need:

```sql
"schema" = "request_data STRUCT<device_data STRUCT<platform VARCHAR(64)>, now BIGINT>"
```

This tells StarRocks to:

- Read only the `request_data` column  
- Within `device_data`, extract only `platform` (ignoring `os_version`, `device_id`, `manufacturer`)  
- Ignore the entire `user_data` struct  
- Extract `now` at the top level of `request_data`

### Step 3: Construct the FILES query with `schema`

```sql
SELECT
    COALESCE(clcode.advertisement.ad_experiences_data.ad_template_name, '') AS ad_template_name,
    COALESCE(clcode.placement.placement_type, '') AS placement_type
FROM FILES(
    "path" = "gs://your-bucket/path/*.parquet",
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "format" = "parquet",
    "schema" = "clcode STRUCT<advertisement STRUCT<ad_experiences_data STRUCT<ad_template_name VARCHAR(64)>>, placement STRUCT<placement_type VARCHAR(16)>>"
);
```

---

## Pros

| Benefit | Description |
| :---- | :---- |
| **Deterministic results** | Schema is fixed regardless of which files are sampled |
| **No merge conflicts** | Bypasses nested struct merge issues entirely |
| **No missing columns** | You declare exactly what you need—no risk of columns missing due to incomplete sampling |
| **Type control** | You choose the StarRocks type—useful when Parquet types vary across files |

## Cons

| Trade-off | Description |
| :---- | :---- |
| **Manual maintenance** | If upstream data adds new fields or changes types, you must update the `schema` parameter |
| **Upfront knowledge required** | You need to know the field paths and types beforehand |
| **Verbosity** | For deeply nested fields, the schema string can be long |

## When to Use This Approach

| Scenario | Recommendation |
| :---- | :---- |
| Fields exist in all files with consistent types | `auto_detect_sample_files=1` may be sufficient |
| Fields exist but types vary across files | **Use explicit schema** |
| Fields exist only in some files | **Use explicit schema** |
| Need stable, reproducible schema across runs | **Use explicit schema** |

