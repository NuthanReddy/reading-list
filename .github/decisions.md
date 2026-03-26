# Organization Decisions

This file records key repository organization decisions for `reading-list`.

## DEC-001: Subject-based foldering
- Date: 2026-03-26
- Status: Accepted
- Decision: Classify books/resources by primary subject folder (for example `Machine_learning`, `System Design`, `Programming`).
- Rationale: Improves discoverability and keeps browsing simple.

## DEC-002: Single best-fit location per file
- Date: 2026-03-26
- Status: Accepted
- Decision: Keep each file in one best-fit folder unless duplication is explicitly requested.
- Rationale: Avoids drift and duplicate maintenance overhead.

## DEC-003: Root stays minimal
- Date: 2026-03-26
- Status: Accepted
- Decision: Keep repository root for metadata files and top-level topic folders (for example `README.md`, `CATALOG.md`).
- Rationale: Reduces clutter and makes the structure predictable.

## DEC-004: Catalog as current inventory
- Date: 2026-03-26
- Status: Accepted
- Decision: Maintain `CATALOG.md` to reflect current folder contents after organization changes.
- Rationale: Gives a quick, searchable snapshot of the library.

## DEC-005: AI folder naming standardization
- Date: 2026-03-26
- Status: Accepted
- Decision: Use `Artificial_Intelligence` (corrected from `Aritificial_Intelligence`).
- Rationale: Fixes typo and keeps naming consistent.

## DEC-006: CleverAlgorithms classification
- Date: 2026-03-26
- Status: Accepted
- Decision: Store `CleverAlgorithms.pdf` under `Machine_learning` instead of `Python`.
- Rationale: Topic fits ML algorithms better than language-specific grouping.

