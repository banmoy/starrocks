# StarRocks Frontend Java Code Refactoring & Documentation Workflow

You are an expert Java programmer with extensive experience in writing high-quality, clear, and maintainable code. Your task is to refactor and document specific Java source files in the StarRocks frontend (`fe/`) following a strict, step-by-step process.

**Our Workflow:**

**Step 0: Workspace Setup**

Based on the initial parameters, I will set up the workspace.

* **If a `WORKTREE_NAME` is provided:**
    1. I will create a new git worktree at the location defined by `{WORKTREE_DIR}/{WORKTREE_NAME}`.
    2. The base for the worktree will be `WORKTREE_BASE` if provided; otherwise, it will be the current active branch.
    3. **Action:** Immediately after creation, I will determine and internally store the **full, absolute path** to this new worktree.
    4. **Verification:** To confirm the context switch, I will immediately perform a `list_dir` operation on the root of the new worktree path.
    5. **CRITICAL MANDATE:** For all subsequent file operations (reading, editing, listing), you MUST use the full, absolute path. This path MUST be constructed by combining the stored absolute worktree path with the file's relative path (e.g., `/Users/lpf/.cursor/worktrees/starrocks/my-worktree/fe/fe-core/src/main/java/com/starrocks/sql/analyzer/Analyzer.java`). You are explicitly forbidden from using relative paths that could resolve to the original project directory when performing file modifications.

* **If `WORKTREE_NAME` is not provided:** I will perform all operations within the existing project workspace.

**Step 1: Target Identification and Scope Definition**

I will identify the target file(s) and define the precise scope of our work based on the `TARGET_INPUT` parameter.

* **If `TARGET_INPUT` is a function name (format: `ClassName#methodName`):**
    1. I will search the codebase to locate the corresponding Java file.
    2. The **scope of change** will be strictly limited to the signature and body of this specific function.

* **If `TARGET_INPUT` is a class name:**
    1. I will search the codebase to locate the corresponding Java file.
    2. The **scope of change** will be the entire class, including its definition, all its member variables, and all its methods.

* **If `TARGET_INPUT` is a commit ID:**
    1. I will inspect the commit to identify all modified Java files and the **specific lines that were changed** within each file.
    2. The **scope of change** for each file will be strictly limited to these changed lines.

**Action:** Present the identified target(s) and the defined scope of change to me for review.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before proceeding to Step 2.

**Step 2: Concept & Keyword Analysis**

Before performing any actual refactoring, I will analyze the semantic consistency of names within the identified scope.

1. **Keyword Scan:** I will identify recurring keywords or nouns used in variable, method, and class names within the scope.
2. **Ambiguity Check:** I will verify if the same keyword is used to represent **different concepts** or logical entities in different contexts.
    * *Example:* Does `id` refer to a *tableId* in one place and a *partitionId* in another?
    * *Example:* Does `state` refer to an *enum value* in one variable but a *complex object* in another?
    * *Example:* Does `ctx` refer to a *ConnectContext* or an *AnalyzeState*?
3. **Conflict Resolution:** If different concepts share the same name, I will flag these as potential sources of confusion.
    * **Strategy:** I will propose adding specific modifiers or qualifiers to differentiate them (e.g., `tableId` vs `partitionId`, `connectCtx` vs `analyzeState`).

**Action:** I will present a report listing any ambiguous keywords found, along with my specific suggestions for resolving them.

**CRITICAL:** You must STOP after this step. I will wait for your review and approval of these conceptual clarifications before proceeding to Step 3.

**Step 3: Naming Refactoring**

Upon approval of the concept analysis, I will refactor the class, method, and variable names strictly within the approved scope.

* **Scope Limitation:** All refactoring is confined to the scope defined and approved in Step 1.
* **Conceptual Clarity:** Ensure names clearly represent the underlying concepts and domain logic (implementing the decisions from Step 2).

**Naming Priority (in order):**
1. **Clarity First:** The name must unambiguously convey the identifier's purpose. Never sacrifice clarity for brevity.
2. **Conciseness Second:** Once clarity is ensured, prefer shorter names over longer ones.
3. **Consistency Third:** Align with naming patterns already established in the surrounding code.

**Length Guidelines:**

| Identifier Type | Recommended Max Length | Hard Limit |
|-----------------|------------------------|------------|
| Local variables | 15 characters | 25 characters |
| Method names | 25 characters | 35 characters |
| Class names | 30 characters | 40 characters |
| Constants | 30 characters | 40 characters |

**Abbreviation Policy:**
* **Encouraged abbreviations** (use freely):
  * `ctx` (context), `cfg` (config), `conn` (connection), `stmt` (statement)
  * `expr` (expression), `idx` (index), `len` (length), `msg` (message)
  * `req` (request), `resp` (response), `txn` (transaction), `util` (utility)
  * `impl` (implementation), `info` (information), `spec` (specification)
  * `attr` (attribute), `param` (parameter), `args` (arguments), `tmp` (temporary)
  * `prev` (previous), `cur`/`curr` (current), `num` (number), `max`/`min`
  * `src` (source), `dst` (destination), `err` (error), `val` (value)
  * `mgr` (manager), `svc` (service), `repo` (repository), `ref` (reference)
* **Avoid** domain-specific or uncommon abbreviations unless they are already prevalent in the codebase.
* **Never abbreviate** if it creates ambiguity (e.g., `cnt` could mean "count" or "content").

**When Clarity and Brevity Conflict:**
* If a short name is ambiguous, choose the longer, clearer name.
* *Example:* Prefer `partitionId` over `partId` if `partId` could be confused with "partial ID".
* *Example:* Prefer `isTransactionCommitted` over `isTxnDone` if "done" is ambiguous (could mean committed, aborted, or timed out).

**StarRocks Java Naming Conventions:**
* **Classes/Interfaces/Enums:** `UpperCamelCase` (e.g., `QueryPlanner`, `ScalarOperator`, `AnalyzeState`)
* **Methods:** `lowerCamelCase` (e.g., `getPartitionInfo`, `rewriteExpression`)
* **Member Variables:** `lowerCamelCase` (e.g., `tableId`, `connectContext`)
* **Local Variables/Parameters:** `lowerCamelCase` (e.g., `columnRef`, `slotDescriptor`)
* **Constants:** `UPPER_SNAKE_CASE` (e.g., `MAX_PARTITION_NUM`, `DEFAULT_TIMEOUT_MS`)
* **Consistency:** Ensure all usages of a renamed identifier are updated throughout the file.

**Action:** Present the refactored code to me for review.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before proceeding to Step 4.

**Step 4: Code Documentation**

After I review and approve the refactoring, you will add comprehensive Javadoc comments to the code, again, strictly within the approved scope.

* **Scope Limitation:** Only add or update documentation for code within the scope defined in Step 1.

#### Documentation Logic & Content

1. **Methods:**
    * **Skip** documentation for simple, short methods where the logic is self-evident (e.g., simple getters or clear 1-2 line helpers).
    * **Mandatory** documentation is required if:
        * The method throws an exception (checked or unchecked). You MUST explain the `@throws` behavior.
        * The return value requires specific explanation. This applies if the return value can be `null`, or if it carries special semantic meaning (e.g., specific status codes, sentinel values, or complex state indicators).

2. **Inline Comments:**
    * **For Long Functions:** You MUST add simple inline comments to demarcate and explain each distinct step or logical block within the function's execution flow.
    * **General Rule:** Use inline comments sparingly elsewhere, only for non-obvious logic.

3. **Classes (if in scope):**
    * Include a summary and usage scenario.

4. **Member Variables (if in scope):**
    * Provide a brief one-sentence summary of its purpose.

5. **Existing Documentation:**
    * Review existing comments within the scope. You **MUST** correct them if they contain obvious grammatical errors, typos, or if the logic described contradicts the actual code behavior.

#### General Rules

* All comments must be written in professional English.
* Use Javadoc style (`/** ... */`) for classes and methods.
* Comment format:

```java
/**
 * Brief description of the method.
 *
 * @param paramName Description of the parameter.
 * @return Description of return value, especially if nullable or has special meaning.
 * @throws ExceptionType Description of when/why this exception is thrown.
 */
public ReturnType methodName(ParamType paramName) throws ExceptionType {
```

**Action:** Present the documented code to me for review.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before proceeding to Step 5.

**Step 5: Logging & Message Refactoring**

Finally, I will refine all log statements, exception messages, and custom status messages within the scope.

* **Refinement:** Improve existing logs and messages for clarity, grammar, and conciseness. Ensure they contain necessary context (IDs, states) without being verbose.
* **Coverage:** Add necessary logs to aid troubleshooting, particularly in exception handlers or complex logic branches.
* **StarRocks Logging:**
  * Use `LOG.debug()`, `LOG.info()`, `LOG.warn()`, `LOG.error()` from Log4j2.
  * Use parameterized logging: `LOG.debug("Processing table {}", tableId)` instead of string concatenation.
* **Level Policy:**

| Level | Usage |
|-------|-------|
| **ERROR** | Exceptions, failures, or critical unexpected states that require immediate attention. |
| **WARN** | Recoverable issues, degraded states, or conditions that may lead to errors. |
| **INFO** | Use **extremely sparingly**—only for significant system events. Never for per-query operations. |
| **DEBUG** | Normal execution flow tracking, detailed state information for troubleshooting. |
| **TRACE** | Very fine-grained diagnostic information, typically disabled in production. |

* **Performance Considerations:**
  * **For INFO/WARN/ERROR:** Ensure messages are lightweight. Do **not** call expensive `toString()` on large objects.
  * **For DEBUG/TRACE:** Richer information is acceptable, but guard with `if (LOG.isDebugEnabled())` if the message construction is expensive:

```java
if (LOG.isDebugEnabled()) {
    LOG.debug("Expensive debug info: {}", expensiveToString());
}
```

* **Anti-Patterns to Avoid:**
  * Duplicate logging (catch exception, log it, re-throw without wrapping).
  * Logging sensitive information (passwords, tokens, PII).
  * Using string concatenation instead of parameterized logging.
  * Missing context in error messages (e.g., "failed to get partition" without the partition ID).

**Action:** Present the code with updated logging/messages for review.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before proceeding to Step 6.

**Step 6: Documentation Sync Check**

After approving the logging/message changes, I will assess whether any modifications require updates to the project's user-facing documentation.

**Triggers for Documentation Updates:**

| Change Type | Documentation Action Required |
|-------------|------------------------------|
| User-visible error message text modified | Review troubleshooting docs |
| System variable / config property changed | Update configuration reference |
| SQL function behavior or signature affected | Update function documentation |
| Public API endpoint / format changed | Update API documentation |
| New feature added | Create new documentation page |

**Documentation Locations (StarRocks-specific):**

| Language | Directory | Sync Requirement |
|----------|-----------|------------------|
| English | `docs/en/` | Primary source of truth |
| Chinese | `docs/zh/` | Must stay synchronized with English |
| Japanese | `docs/ja/` | Must stay synchronized with English |

**Process:**

1. **Impact Assessment:** I will analyze the changes made in Steps 3-5 and determine if any user-facing strings, error messages, or documented behaviors were affected.

2. **If Documentation Updates Are Needed:**
    * I will identify the specific documentation files that require changes.
    * I will present the list of affected files and proposed changes for your review.
    * **Multilingual Requirement:** If updating documentation, I will prepare changes for **all three languages** (EN, ZH, JA) to maintain consistency.

3. **If No Documentation Updates Are Needed:**
    * I will explicitly state that no user-facing changes were detected and documentation sync is not required.

**Documentation Standards:**
* **Sidebar Registration:** New pages must be added to `docs/docusaurus/sidebars.json`.
* **Code Blocks:** Always include a language identifier (e.g., ```sql, ```java).
* **Consistency:** Ensure terminology changes in code are reflected consistently across all language versions.

**Action:** Present my documentation impact assessment. If updates are needed, present the proposed documentation changes for all affected languages.

**CRITICAL:** You must STOP after this step and wait for my explicit approval before finalizing.

**Step 7: Finalization**

Upon approval of all changes (code and documentation), I will:

1. **Generate a Change Summary:**
    * List all modified files (code and documentation).
    * Summarize key naming changes in a table format:
    
    | Original Name | New Name | Location |
    |---------------|----------|----------|
    | `oldName` | `newName` | `ClassName.java:123` |
    
    * Note any documentation files updated.

2. **If Working in a Worktree:**
    * Ask if you want me to create a commit with the changes.
    * Suggest a commit message following the project's PR title format:
    
    ```
    [Refactor] <brief description of the refactoring>
    
    - Improved naming clarity in XxxClass
    - Added Javadoc documentation
    - Updated error messages for better debugging
    ```

3. **Provide Next Steps:**
    * Remind about running relevant unit tests.
    * If cross-file references exist (e.g., renamed public methods), list files that may need manual review.
    * Suggest creating a PR if the changes are ready for review.

**Action:** Present the final summary and await your confirmation to conclude the task.

---

**TO START THE TASK, PLEASE PROVIDE THE FOLLOWING PARAMETERS:**

* `WORKTREE_DIR`: [Optional: The directory where the worktree will be created. Defaults to `/Users/lpf/.cursor/worktrees/starrocks`.]
* `WORKTREE_NAME`: [Optional: The name for the new git worktree. Leave empty to work in the current directory.]
* `WORKTREE_BASE`: [Optional: The branch name or commit ID to create the worktree from. If `WORKTREE_NAME` is specified and this is empty, the current branch will be used as the base.]
* `TARGET_INPUT`: [Required: The Java class name, function name (format: `ClassName#methodName`), or commit ID to be refactored and documented.]
* `TARGET_FILE`: [Optional: Direct file path to avoid search ambiguity.]
