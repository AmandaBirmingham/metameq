# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

METAMEQ (Metadata Extension Tool to Annotate Microbiome Experiments for Qiita) is a Python tool that extends tabular metadata files with standardized annotation columns required for submission to Qiita and EBI. Users annotate their metadata with two shorthand columns (`hosttype_shorthand` for host organism, `sampletype_shorthand` for sample classification), and the tool expands these into the full set of standardized metadata fields.

## Build & Development Commands

```bash
# Environment setup
conda env create -n metameq -f environment.yml
conda activate metameq
pip install -e .

# Run all tests
pytest

# Run a single test file
pytest metameq/tests/test_metadata_extender.py

# Run a single test function
pytest metameq/tests/test_metadata_extender/test_group_io_and_results.py::test_write_extended_metadata

# Regenerate golden output files for type pair tests
pytest metameq/tests/test_standards_all_type_pairs.py --update-golden

# Linting (CI uses flake8, max-line-length=127)
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
```

## CLI Usage

```bash
# Extend and validate metadata using a study-specific config and the built-in standards
metameq write-extended-metadata METADATA_FILE CONFIG_FILE NAME_BASE [--out_dir DIR] [--sep SEP] [--suppress_fails_files]

# Extend and validate metadata using a pre-built full flat config
metameq write-validator-metadata METADATA_FILE FULL_FLAT_CONFIG_FILE NAME_BASE [--out_dir DIR] [--sep SEP] [--keep_internals] [--suppress_fails_files] [--hosttype_col_name NAME] [--sampletype_col_name NAME]
```

## Architecture

### Data Flow

```
Input (XLSX/CSV/TXT) → Load with encoding detection → Validate required columns
→ Load standards.yml + study config → Combine & flatten configurations
→ Extend metadata based on host/sample types → Apply transformers
→ Validate → Output (extended metadata + validation errors + QC failures)
```

### Configuration Hierarchy

The configuration system has four layers that get merged and flattened:

1. **Standards config** (`standards.yml`, ~73KB) — hierarchical host/sample type definitions with EBI-compliant metadata fields. Uses YAML anchors (with the naming format `_reusable_definitions`) for shared definitions. Organized by `host_type_specific_metadata` → host type → `sample_type_specific_metadata` → sample type → `metadata_fields`.
2. **Software config** (`config.yml`, shipped with package) — default settings loaded via `extract_config_dict(None)`.
3. **Study config** (user-provided YAML) — study-specific overrides under `study_specific_metadata`, plus `metadata_transformers` and top-level settings like `leave_requireds_blank`, `overwrite_non_nans`. Merged on top of software config defaults before combining with standards.
4. **Flat config** — the merged result: a dict keyed by `(host_type, sample_type)` tuples, each containing a flat `metadata_fields` dict plus resolved transformers. Produced by `build_full_flat_config_dict()`.

In 2 and 3, alternative shorthand column names for host types and sample types may be specified in `hosttype_column_options` and `sampletype_column_options`, respectively.

In 1 and 3, sample types support inheritance via `base_type` and `alias` keys. 

### Core Modules (`metameq/src/`)

- **metadata_configurator.py** — standards + study config merger, sample type inheritance resolver (`base_type`/`alias`), nested-to-flat hierarchy converter producing `(host, sample)` keyed dicts
- **metadata_extender.py** — main orchestrator: input loading, flat config building, per-row metadata extension by host+sample type, transformer application, QC/validation running, output writing
- **metadata_merger.py** — DataFrame join utilities for combining sample and subject metadata (many-to-one and one-to-one merges)
- **metadata_transformers.py** — row-level transformation functions (sex standardization, age-to-life-stage, date formatting, pass-through, mapping) invoked by name from config
- **metadata_validator.py** — Cerberus-based validation layer with custom `MetameqValidator` class and per-row error message generation
- **util.py** — shared constants (all `*_KEY` names), config loading helpers, encoding-aware file reader, type casting utilities

### Transformer System

Transformers are functions in `metadata_transformers.py` referenced by name in config YAML. They are split into `pre_transformers` (run before metadata extension) and `post_transformers` (run after). Each transformer entry specifies a `function` name and `sources` list of input column names. Study configs can add or override transformers.

### Test Structure

Tests are organized into subdirectories mirroring the modules they test:

- `tests/test_metadata_configurator/` — tests split by function group, with shared fixtures in `conftest.py`
- `tests/test_metadata_extender/` — tests split by function group, with shared fixtures in `conftest.py`
- `tests/test_metadata_merger.py`, `test_metadata_transformers.py`, `test_metadata_validator.py`, `test_util.py` — single-file test modules
- `tests/test_standards_all_type_pairs.py` — golden file tests that verify output for every host+sample type pair in standards.yml; golden CSVs stored in `tests/data/expected_type_pair_outputs/`
- `tests/data/` — shared test fixtures (YAML configs, CSV/TSV/XLSX inputs, expected outputs)

### Key Constants (defined in `util.py`)

- Required input columns: `sample_name`, `hosttype_shorthand`, `sampletype_shorthand`
- Special values: `LEAVE_BLANK_VAL` ("leaveblank"), `DO_NOT_USE_VAL` ("donotuse"), `NOT_PROVIDED_VAL` ("not provided")

## Dependencies

- Core: click, pandas, openpyxl, PyYAML, cerberus, python-dateutil
- Python: 3.9, 3.10
- CI: GitHub Actions with conda, flake8, pytest
