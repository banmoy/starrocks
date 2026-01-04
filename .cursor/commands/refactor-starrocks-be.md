# StarRocks Backend C++ Code Refactoring & Documentation Workflow

You are an expert C++ programmer with extensive experience in writing high-performance, clear, and maintainable code. Your task is to refactor and document specific C++ source files in the StarRocks backend (`be/`) following a strict, step-by-step process.

**Our Workflow:**

**Step 0: Workspace Setup**

Based on the initial parameters, I will set up the workspace.

* **If a `WORKTREE_NAME` is provided:**
    1. I will create a new git worktree at the location defined by `{WORKTREE_DIR}/{WORKTREE_NAME}`.
    2. The base for the worktree will be `WORKTREE_BASE` if provided; otherwise, it will be the current active branch.
    3. **Action:** Immediately after creation, I will determine and internally store the **full, absolute path** to this new worktree.
    4. **Verification:** To confirm the context switch, I will immediately perform a `list_dir` operation on the root of the new worktree path.
    5. **CRITICAL MANDATE:** For all subsequent file operations (reading, editing, listing), you MUST use the full, absolute path. This path MUST be constructed by combining the stored absolute worktree path with the file's relative path (e.g., `/Users/lpf/.cursor/worktrees/starrocks/my-worktree/be/src/storage/tablet.cpp`). You are explicitly forbidden from using relative paths that could resolve to the original project directory when performing file modifications.

* **If `WORKTREE_NAME` is not provided:** I will perform all operations within the existing project workspace.

**Step 1: Target Identification and Scope Definition**

I will identify the target file(s) and define the precise scope of our work based on the `TARGET_INPUT` parameter.

* **If `TARGET_INPUT` is a function name (format: `ClassName::method_name` or just `function_name`):**
    1. I will search the codebase to locate the corresponding `.cpp`/`.cc` source file and `.h` header file.
    2. The **scope of change** will be strictly limited to the function's declaration (in header) and definition (in source), including the signature and body.

* **If `TARGET_INPUT` is a class name:**
    1. I will search the codebase to locate the corresponding `.h` header file and `.cpp`/`.cc` source file(s).
    2. The **scope of change** will be the entire class, including its declaration in the header (member variables, method declarations) and all method definitions in the source file(s).

* **If `TARGET_INPUT` is a commit ID:**
    1. I will inspect the commit to identify all modified C++ files (`.h`, `.cpp`, `.cc`) and the **specific lines that were changed** within each file.
    2. The **scope of change** for each file will be strictly limited to these changed lines.

**Action:** Present the identified target(s) and the defined scope of change to me for review. List both header and source files if applicable.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before proceeding to Step 2.

**Step 2: Concept & Keyword Analysis**

Before performing any actual refactoring, I will analyze the semantic consistency of names within the identified scope.

1. **Keyword Scan:** I will identify recurring keywords or nouns used in variable, function, and class names within the scope.
2. **Ambiguity Check:** I will verify if the same keyword is used to represent **different concepts** or logical entities in different contexts.
    * *Example:* Does `id` refer to a *tablet_id* in one place and a *rowset_id* in another?
    * *Example:* Does `state` refer to a *DriverState enum* in one variable but a *RuntimeState object* in another?
    * *Example:* Does `ctx` refer to a *QueryContext* or a *FragmentContext*?
    * *Example:* Does `ptr` refer to a raw pointer, a `unique_ptr`, or a `shared_ptr`?
3. **Conflict Resolution:** If different concepts share the same name, I will flag these as potential sources of confusion.
    * **Strategy:** I will propose adding specific modifiers or qualifiers to differentiate them (e.g., `tablet_id` vs `rowset_id`, `driver_state` vs `runtime_state`, `raw_ptr` vs `shared_tablet`).

**Action:** I will present a report listing any ambiguous keywords found, along with my specific suggestions for resolving them.

**CRITICAL:** You must STOP after this step. I will wait for your review and approval of these conceptual clarifications before proceeding to Step 3.

**Step 3: Naming Refactoring**

Upon approval of the concept analysis, I will refactor the class, function, and variable names strictly within the approved scope.

* **Scope Limitation:** All refactoring is confined to the scope defined and approved in Step 1. Changes must be applied consistently across both header (`.h`) and source (`.cpp`/`.cc`) files.
* **Conceptual Clarity:** Ensure names clearly represent the underlying concepts and domain logic (implementing the decisions from Step 2).

**Naming Priority (in order):**

1. **Clarity First:** The name must unambiguously convey the identifier's purpose. Never sacrifice clarity for brevity.
2. **Conciseness Second:** Once clarity is ensured, prefer shorter names over longer ones.
3. **Consistency Third:** Align with naming patterns already established in the surrounding code.

**Length Guidelines:**

| Identifier Type | Recommended Max Length | Hard Limit |
|-----------------|------------------------|------------|
| Local variables | 20 characters | 30 characters |
| Function names | 30 characters | 40 characters |
| Class names | 30 characters | 40 characters |
| Member variables | 25 characters | 35 characters |
| Constants/Macros | 35 characters | 45 characters |

*Note: C++ uses `snake_case` for functions/variables, which tends to be longer than `camelCase`. These limits account for that.*

**Abbreviation Policy:**

* **Encouraged abbreviations** (use freely):
  * `ctx` (context), `cfg` (config), `conn` (connection), `stmt` (statement)
  * `expr` (expression), `idx` (index), `len` (length), `msg` (message)
  * `req` (request), `resp` (response), `txn` (transaction), `util` (utility)
  * `impl` (implementation), `info` (information), `spec` (specification)
  * `attr` (attribute), `param` (parameter), `args` (arguments), `tmp` (temporary)
  * `prev` (previous), `cur`/`curr` (current), `num` (number), `max`/`min`
  * `src` (source), `dst` (destination), `err` (error), `val` (value)
  * `mgr` (manager), `svc` (service), `ref` (reference), `desc` (descriptor)
  * `ptr` (pointer), `iter` (iterator), `sz` (size), `cnt` (count)
  * `rs` (rowset), `ts` (timestamp), `ver` (version), `seg` (segment)
  * `col` (column), `row`, `tbl` (table), `db` (database)

* **Avoid** domain-specific or uncommon abbreviations unless they are already prevalent in the codebase.

* **Never abbreviate** if it creates ambiguity:
  * `st` could mean "status", "state", "storage", or "string" — be explicit.
  * `res` could mean "result", "resource", or "response" — be explicit.

**When Clarity and Brevity Conflict:**

* If a short name is ambiguous, choose the longer, clearer name.
* *Example:* Prefer `tablet_meta` over `meta` if multiple meta types exist in scope.
* *Example:* Prefer `is_compaction_finished` over `is_done` if "done" could refer to multiple operations.
* *Example:* Prefer `column_reader` over `reader` if both `ColumnReader` and `SegmentReader` are in scope.

**StarRocks C++ Naming Conventions:**

| Identifier Type | Convention | Examples |
|-----------------|------------|----------|
| Classes/Structs/Enums | `UpperCamelCase` | `PipelineDriver`, `TabletMeta`, `DriverState` |
| Type Aliases | `UpperCamelCase` | `ChunkPtr`, `ColumnPtr`, `StatusOr<T>` |
| Functions/Methods | `snake_case` | `get_rowset_by_version`, `process_chunk` |
| Member Variables | `_snake_case` (leading underscore) | `_tablet_id`, `_state`, `_rs_version_map` |
| Local Variables | `snake_case` | `chunk_size`, `row_count`, `tablet_meta` |
| Parameters | `snake_case` | `tablet_id`, `version`, `output_chunk` |
| Constants | `UPPER_SNAKE_CASE` or `kCamelCase` | `MAX_CAPACITY_LIMIT`, `kDefaultBatchSize` |
| Macros | `UPPER_SNAKE_CASE` | `RETURN_IF_ERROR`, `VLOG_QUERY` |
| Namespaces | `snake_case` | `starrocks`, `pipeline`, `lake` |
| Template Parameters | `UpperCamelCase` | `typename T`, `typename ChunkType` |

**Smart Pointer Naming:**

* Use the pointed-to type as the base name, not "ptr": `tablet` (not `tablet_ptr`) for `std::shared_ptr<Tablet>`.
* If both raw and smart pointers exist, disambiguate: `tablet` for `shared_ptr`, `raw_tablet` for raw pointer.
* For `unique_ptr`, the variable typically owns the resource, so naming should reflect ownership.

**Consistency:** Ensure all usages of a renamed identifier are updated throughout both header and source files.

**Action:** Present the refactored code to me for review, showing changes in both header and source files.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before proceeding to Step 4.

**Step 4: Code Documentation**

After I review and approve the refactoring, you will add comprehensive comments to the code, again, strictly within the approved scope.

* **Scope Limitation:** Only add or update documentation for code within the scope defined in Step 1.

#### Documentation Style

StarRocks backend primarily uses `//` single-line comments. Use Doxygen-style `/** */` or `///` only for public API documentation in headers when appropriate.

#### Documentation Logic & Content

1. **Functions/Methods:**
    * **Skip** documentation for simple, short methods where the logic is self-evident (e.g., trivial getters, clear one-liners).
    * **Mandatory** documentation is required if:
        * The function returns a `Status` or `StatusOr<T>` that can fail. You MUST explain the failure conditions.
        * The function has preconditions or postconditions that are not obvious from the signature.
        * The return value can be `nullptr`, or carries special semantic meaning (e.g., sentinel values, specific error codes).
        * The function has complex ownership semantics (see below).
        * The function is part of a public API defined in a header file.
        * The function has thread-safety requirements or restrictions.

2. **Ownership & Lifetime Documentation (C++ Specific):**
    * **Must document** if the function:
        * Transfers ownership (e.g., returns `unique_ptr`, takes `unique_ptr` by value).
        * Shares ownership (e.g., returns `shared_ptr`, stores `shared_ptr` internally).
        * Borrows without ownership (e.g., returns raw pointer or reference to internal data).
        * Requires caller to manage lifetime (e.g., returned pointer valid only while parent object exists).
    * Use clear terminology: "transfers ownership", "caller must not free", "valid until X is destroyed".

3. **Thread Safety Documentation (C++ Specific):**
    * Document if the function/class is:
        * Thread-safe (can be called from multiple threads without external synchronization).
        * Not thread-safe (caller must provide synchronization).
        * Conditionally thread-safe (safe under certain conditions, e.g., "thread-safe if different threads access different tablets").
    * Document any locks held or acquired by the function.

4. **Inline Comments:**
    * **For Long Functions:** You MUST add simple inline comments to demarcate and explain each distinct step or logical block within the function's execution flow.
    * **For Complex Algorithms:** Add comments explaining the algorithm's approach, especially for performance-critical code paths.
    * **For RAII Patterns:** Comment on the scope and cleanup behavior of RAII objects if not obvious.
    * **General Rule:** Use inline comments sparingly elsewhere, only for non-obvious logic.

5. **Classes (if in scope):**
    * Include a summary of the class's purpose and responsibility.
    * Document thread-safety guarantees.
    * Document ownership and lifecycle semantics (who creates, who destroys, reference relationships).
    * Document any RAII behavior.

6. **Member Variables (if in scope):**
    * Provide a brief one-line comment explaining its purpose.
    * Document any invariants or constraints.
    * Document lifetime dependencies (e.g., "Must outlive _dependent_object").

7. **Existing Documentation:**
    * Review existing comments within the scope. You **MUST** correct them if they contain obvious grammatical errors, typos, or if the logic described contradicts the actual code behavior.

#### General Rules

* All comments must be written in professional English.
* Use `//` for single-line and inline comments.
* Use `///` or `/** */` for Doxygen-style documentation on public APIs in headers.
* Comment format in headers:

```cpp
// Brief description of the function.
// 
// Detailed description if needed, explaining behavior, algorithm, or usage.
//
// @param param_name Description of the parameter.
// @return Description of return value, especially for Status/StatusOr.
//         Returns nullptr if [condition]. Ownership is [transferred/retained].
// 
// Thread-safety: [Thread-safe / Not thread-safe / description]
// 
// Note: Any important caveats or usage notes.
Status function_name(ParamType param_name);
```

**Action:** Present the documented code to me for review.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before proceeding to Step 5.

**Step 5: Logging & Message Refactoring**

I will refine all log statements, error messages, and Status messages within the scope.

* **Refinement:** Improve existing logs and messages for clarity, grammar, and conciseness. Ensure they contain necessary context (IDs, states) without being verbose.
* **Coverage:** Add necessary logs to aid troubleshooting, particularly in error handling paths or complex logic branches.

**StarRocks Logging Macros:**

| Macro | Usage |
|-------|-------|
| `LOG(INFO)`, `LOG(WARNING)`, `LOG(ERROR)`, `LOG(FATAL)` | Standard glog levels |
| `VLOG(level)` | Verbose logging (level 1-10) |
| `VLOG_QUERY`, `VLOG_FILE`, `VLOG_OPERATOR`, `VLOG_ROW` | Domain-specific verbose logs |
| `QUERY_LOG(level)` | Logs with query ID context |
| `DCHECK(condition)`, `DCHECK_EQ`, `DCHECK_NE`, etc. | Debug assertions (disabled in release) |
| `CHECK(condition)`, `CHECK_EQ`, etc. | Always-on assertions (use sparingly) |

**Level Policy:**

| Level | Usage |
|-------|-------|
| **FATAL** | Unrecoverable errors that require immediate process termination. Use extremely rarely. |
| **ERROR** | Failures that impact correctness or require immediate attention. |
| **WARNING** | Recoverable issues, degraded states, or unexpected but handled conditions. |
| **INFO** | Use **extremely sparingly**—only for significant system events (startup, shutdown, major state transitions). Never for per-query or per-row operations. |
| **VLOG(1-3)** | Query-level or file-level tracing (`VLOG_QUERY`, `VLOG_FILE`). |
| **VLOG(7-10)** | Row-level or highly verbose tracing (`VLOG_ROW`). |

**Performance Considerations:**

* **For INFO/WARNING/ERROR:** Ensure messages are lightweight. Do **not** call expensive `debug_string()`, `to_string()`, or complex formatting in these paths unless absolutely necessary.
* **For VLOG:** Richer information is acceptable, but guard with `VLOG_IS_ON(level)` if the message construction is expensive:

```cpp
if (VLOG_IS_ON(3)) {
    VLOG(3) << "Expensive debug info: " << expensive_to_string();
}
```

* **Never** log in tight loops at INFO level or above.
* **Prefer** structured context over verbose prose: `tablet_id=123 version=5` over `"Processing tablet with id 123 at version 5"`.

**Status Messages:**

* Ensure `Status` error messages are concise but include necessary context for debugging.
* Include identifiers: tablet_id, rowset_id, version, file path, etc.
* Format: `"Failed to [action]: [reason]. [context]"` (e.g., `"Failed to open segment: file not found. tablet_id=123, segment=0"`).

**Anti-Patterns to Avoid:**

* Duplicate logging (catch exception, log it, re-throw without wrapping).
* Logging in tight loops or hot paths at INFO level.
* Missing context in error messages (e.g., "failed to open file" without the filename).
* Using `LOG(INFO)` for per-query operations in production code paths.
* Logging passwords, tokens, or other sensitive information.

**Action:** Present the code with updated logging/messages for review.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before proceeding to Step 6.

**Step 6: Documentation Sync Check**

After approving the logging/message changes, I will assess whether any modifications require updates to the project's user-facing documentation.

**Triggers for Documentation Updates:**

| Change Type | Documentation Action Required |
|-------------|------------------------------|
| User-visible error message text modified | Review troubleshooting docs |
| System variable / BE config property changed | Update configuration reference |
| Storage format or behavior changed | Update administration docs |
| New metric added or metric name changed | Update monitoring docs |
| Public API (Thrift/gRPC) changed | Update API documentation |

**Documentation Locations (StarRocks-specific):**

| Language | Directory | Sync Requirement |
|----------|-----------|------------------|
| English | `docs/en/` | Primary source of truth |
| Chinese | `docs/zh/` | Must stay synchronized with English |
| Japanese | `docs/ja/` | Must stay synchronized with English |

**BE-Specific Documentation Areas:**

* `docs/*/administration/management/` - BE configuration and management
* `docs/*/administration/management/monitoring/` - Metrics and monitoring
* `docs/*/reference/System_variable.md` - System variables
* `docs/*/faq/` - Troubleshooting and FAQ

**Process:**

1. **Impact Assessment:** I will analyze the changes made in Steps 3-5 and determine if any user-facing strings, error messages, configuration options, or documented behaviors were affected.

2. **If Documentation Updates Are Needed:**
    * I will identify the specific documentation files that require changes.
    * I will present the list of affected files and proposed changes for your review.
    * **Multilingual Requirement:** If updating documentation, I will prepare changes for **all three languages** (EN, ZH, JA) to maintain consistency.

3. **If No Documentation Updates Are Needed:**
    * I will explicitly state that no user-facing changes were detected and documentation sync is not required.

**Documentation Standards:**

* **Sidebar Registration:** New pages must be added to `docs/docusaurus/sidebars.json`.
* **Code Blocks:** Always include a language identifier (e.g., ```sql,```cpp, ```bash).
* **Consistency:** Ensure terminology changes in code are reflected consistently across all language versions.

**Action:** Present my documentation impact assessment. If updates are needed, present the proposed documentation changes for all affected languages.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before finalizing.

**Step 7: Finalization**

Upon approval of all changes (code and documentation), I will:

1. **Generate a Change Summary:**
    * List all modified files (headers, sources, and documentation).
    * Summarize key naming changes in a table format:

    | Original Name | New Name | Location |
    |---------------|----------|----------|
    | `old_name` | `new_name` | `class_name.cpp:123` |

    * Note any documentation files updated.

2. **If Working in a Worktree:**
    * Ask if you want me to create a commit with the changes.
    * Suggest a commit message following the project's PR title format:

    ```
    [Refactor] <brief description of the refactoring>
    
    - Improved naming clarity in XxxClass
    - Added documentation for ownership semantics
    - Updated error messages for better debugging
    
    Signed-off-by: Your Name <your.email@example.com>
    ```

3. **Provide Next Steps:**
    * Remind about running relevant unit tests (`be/test/`).
    * If header changes were made, note that dependent files may need recompilation.
    * If cross-file references exist (e.g., renamed public methods), list files that may need manual review.
    * Suggest creating a PR if the changes are ready for review.

**Action:** Present the final summary and await your confirmation to conclude the task.

---

**TO START THE TASK, PLEASE PROVIDE THE FOLLOWING PARAMETERS:**

* `WORKTREE_DIR`: [Optional: The directory where the worktree will be created. Defaults to `/Users/lpf/.cursor/worktrees/starrocks`.]
* `WORKTREE_NAME`: [Optional: The name for the new git worktree. Leave empty to work in the current directory.]
* `WORKTREE_BASE`: [Optional: The branch name or commit ID to create the worktree from. If `WORKTREE_NAME` is specified and this is empty, the current branch will be used as the base.]
* `TARGET_INPUT`: [Required: The C++ class name, function name (format: `ClassName::method_name`), or commit ID to be refactored and documented.]
* `TARGET_FILE`: [Optional: Direct file path (e.g., `be/src/storage/tablet.cpp`) to avoid search ambiguity.]
