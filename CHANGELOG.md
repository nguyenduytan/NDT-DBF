# Changelog

## 0.3.0 - 2026-10-09

### Fixed

- Write operations always use the primary, including manual read routing and scoped transaction clones.
- Removed shared statement caching and automatic re-execution after exceptions; concurrent streams keep independent cursors.
- Corrected NULL inequality, grouped aggregates, qualified soft-delete columns, JSON nested reads and multiple-path updates.
- Bulk inserts normalize column order, return actual numeric IDs, and roll back the whole batch on failure.
- First, pluck and keyset no longer mutate their source builder; keyset respects direction and detects the last page.
- Native PDO injection and SQLite DSN/URI handling; Composer classmap covers both DBF and Query in the single runtime file.
- Unique metadata matches complete unique/primary indexes rather than composite subsets.
- Schema-qualified soft deletes retain rows; joined trashed filters and OR keyset boundaries remain correctly grouped.
- Lost nested transactions preserve the original database failure and block further queries before they can autocommit.
- Query timeouts restore prior connection settings; dry-run streams no longer return a generator as a value.

### Added

- Explicit selectRaw and execute entry points; raw detects returned rows from statement metadata.
- chunkById for processing while deleting rows, callback cancellation, validation and runtime version reporting.
- Regression tests, SQL dialect compilation checks, PHP 8.1-8.5 CI and MySQL/PostgreSQL integration CI.
- Synchronized website source, searchable documentation, release badges and version-pinned downloads.

### Upgrade Notes

- raw always uses the writer and is blocked entirely in readonly mode; use selectRaw or builder reads.
- Regenerate 0.2.x keyset cursors. Use one unique key, select it, order only by that key and set a positive limit.
- insertMany now executes individual inserts inside one transaction to return accurate IDs; use native bulk SQL for maximum throughput.
- Database numeric/decimal types are preserved. Empty avg returns null; grouped numeric aggregates with multiple values require select/get.
- SQL Server/Oracle connection and pagination compilation are available, but transactions, row locks, upsert and JSON operations fail explicitly where unsupported.
- Test mode still connects to a database and can inspect schema; transactions are blocked.

## 0.1.0

- Original single-file PDO framework release.
