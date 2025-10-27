<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/metameq_dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="assets/metameq_light.svg">
  <img alt="METAMEQ logo" src="assets/metameq_medium.png" width="400">
</picture>

### Metadata Extension Tool to Annotate Microbiome Experiments for Qiita

A python tool to extend an existing tabular metadata file by inferring and adding 
the standard annotation columns required for submission to [Qiita](https://qiita.ucsd.edu/) and [EBI](https://www.ebi.ac.uk/).

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [API Usage](#api-usage)

## Overview

METAMEQ (pronounced “meta-mek”) is a Python-based tool designed to help researchers effortlessly generate standards-compliant microbiome sample metadata. Many data collection standards require specific metadata columns and controlled vocabulary values, which can be time-consuming to assemble manually. METAMEQ streamlines this process by letting users annotate their existing tabular metadata with just two shorthand columns: `hosttype_shorthand` (e.g., human, mouse, non-saline water, etc) and `sampletype_shorthand` (e.g., skin, saliva, feces, wastewater, etc). Once the annotated file is loaded into METAMEQ, the tool automatically expands these shorthand entries into the full set of standardized metadata fields required by multiple community standards, outputting a ready-to-use, enriched metadata file suitable for submission to Qiita and/or EBI. This helps ensure interoperability, reproducibility, and compliance with data sharing best practices.

## Installation

To install this package, first clone the repository from GitHub:

```
git clone https://github.com/biocore/metameq.git
```

Change directory into the new `metameq` folder and create a 
Python3 Conda environment in which to run the software:

```
conda env create -n metameq -f environment.yml  
```

Activate the Conda environment and install the package:

```
conda activate metameq
pip install -e .
```

## Basic Usage

METAMEQ is run from the command line using the `metameq` command: 

```bash
metameq write-extended-metadata METADATA_FILE CONFIG_FILE NAME_BASE [OPTIONS]
```

### Required Inputs

1. **METADATA_FILE**: Path to your input metadata file containing sample information
   - Accepted formats: `.csv`, `.txt`, or `.xlsx`
   - Must include columns for `sample_name`, `hosttype_shorthand`, and `sampletype_shorthand`

2. **CONFIG_FILE**: Path to your study-specific configuration YAML file
   - Defines study-specific settings like default values and transformation rules
   - See `config.yml` for an example configuration

3. **NAME_BASE**: Base name suffix for output files
   - Used to generate output filenames, which will be <timestamp>_<basename>.<extension> (e.g., "2024-05-16_09-46-19_mymetadata.csv" for the name base "mymetadata")

### Optional Parameters

- `--out_dir`: Output directory for generated files (default: current directory)
- `--sep`: Separator character for text output files.  If ",", the output will be a `.csv` file, and if "\t" the output will be `.txt` file. "\t" is the default
- `--suppress_fails_files`: Suppress empty QC and validation error files (default: outputs empty files even when no errors found)

### Example

```bash
metameq write-extended-metadata my_samples.xlsx config.yml my_study_name --out_dir ./output
```

This command will:
- Read sample metadata from `my_samples.xlsx`
- Apply configurations from `config.yml`
- Generate extended metadata files with standardized fields based on host and sample types
- Output validation results and QC reports
- Save all outputs to the `./output` directory with the suffix `my_study_name`

## API Usage

METAMEQ can also be imported and used as a Python library within your own code. This is useful for integrating metadata extension into custom workflows or pipelines.

### Core Functions

The primary functions for programmatic use are:

- **`write_extended_metadata_from_df`**: Extend metadata from a pandas DataFrame and write results to files
- **`get_extended_metadata_from_df_and_yaml`**: Extend metadata and return DataFrames without writing to disk
- **`extract_config_dict`**: Load and extract configuration from YAML files

### Basic API Example

```python
import pandas as pd
from metameq import (
    write_extended_metadata_from_df,
    extract_config_dict,
    HOSTTYPE_SHORTHAND_KEY,
    SAMPLETYPE_SHORTHAND_KEY
)

# Load your raw metadata into a DataFrame
raw_metadata_df = pd.read_csv("my_samples.csv")

# Ensure required columns exist
raw_metadata_df[HOSTTYPE_SHORTHAND_KEY] = "human"
raw_metadata_df[SAMPLETYPE_SHORTHAND_KEY] = "stool"

# Load configuration
config_dict = extract_config_dict("config.yml")

# Extend metadata and write output files
extended_df = write_extended_metadata_from_df(
    raw_metadata_df,
    config_dict,
    out_dir="./output",
    out_name_base="my_study"
)
```

### Advanced: Custom Transformers

You can define custom transformation functions for study-specific data processing:

```python
from metameq import transform_date_to_formatted_date

def custom_date_transformer(row, source_fields):
    """Custom function to handle date formatting based on sample type."""
    if row[HOSTTYPE_SHORTHAND_KEY] == "control":
        return row["extraction_date"]
    else:
        return transform_date_to_formatted_date(row, source_fields)

# Pass custom transformers as a dictionary
transformers = {
    "custom_date_transformer": custom_date_transformer
}

extended_df = write_extended_metadata_from_df(
    raw_metadata_df,
    config_dict,
    out_dir="./output",
    out_name_base="my_study",
    study_specific_transformers_dict=transformers
)
```

### Available Utility Functions

METAMEQ exports several utility functions for data handling:

- **Metadata merging**: `merge_sample_and_subject_metadata`, `merge_many_to_one_metadata`, `merge_one_to_one_metadata`
- **File loading**: `load_df_with_best_fit_encoding`
- **Data transformers**: `transform_input_sex_to_std_sex`, `transform_age_to_life_stage`, `format_a_datetime`
- **Validation**: `get_qc_failures`, `id_missing_cols`, `find_standard_cols`, `find_nonstandard_cols`

### Key Constants

METAMEQ provides constants for required column names:

- `HOSTTYPE_SHORTHAND_KEY`: Column name for host type classification
- `SAMPLETYPE_SHORTHAND_KEY`: Column name for sample type classification
- `SAMPLE_NAME_KEY`: Column name for sample identifiers
- `HOST_SUBJECT_ID_KEY`: Column name for subject identifiers
- `QC_NOTE_KEY`: Column name for quality control notes
