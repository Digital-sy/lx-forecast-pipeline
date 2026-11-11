# Garment source date evidence gate

Version: 1.1.0-date-evidence-gate. This is an offline-reviewed candidate, not a deployed scheduler or database repair.

The previous source parser can retain a filename date when an explicitly labelled body date uses compact YYYYMMDD. The new PREPARE orchestration runs a date consistency gate after parsing and before indexing or packaging. Unresolved conflicts, invalid/unparsed body evidence with a selected date, and unsupported selected dates stop the package stage.

An honestly preserved conflict with no selected date may remain raw evidence. Missing body evidence stays unknown. Passing this gate does not establish an effective purchase date, normal purchase status, a material recipe, or a color mapping.

## Scope and prerequisites

- CHECK_ONLY is the default and performs no remote CLI execution.
- DISCOVER and PREPARE require explicit read flags and the existing pinned source tools/configuration.
- Pin all dependency hashes and use a new output directory. This module contains no database client or publish command.
- The existing parser is not silently replaced. A blocked record needs separately reviewed re-parsing/correction; prior raw rows and manual confirmations are retained.
- No deployment, scheduling, production correction, or automatic promotion is included.
- All test fixtures are synthetic. Business workbooks, database snapshots, identities, inputs and results stay private.

## Reproduce offline tests

    python -B -m unittest discover -s tests -p 'test_source*.py' -v

The tests exercise the actual PREPARE function with a fake command runner and confirm that a parsing conflict prevents the index and package tools from being called. They also retain the prior orchestration, hash, scope and manual-reference protections. No live service is contacted.

## Manual local check

Use the existing source pipeline configuration with explicitly reviewed paths and SHA256 values:

    python -B scripts/source_pipeline_v2.py --config <approved-local-config> --output-dir <new-task-output>

Use --help for the exact stage interface. Do not treat this README as authorization to fetch or publish data. The source files are byte-normalized to UTF-8 LF for GitHub; local execution hashes are recorded separately.
