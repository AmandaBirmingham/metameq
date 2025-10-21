![METAMEQ logo](https://raw.githubusercontent.com/AmandaBirmingham/metameq/main/metameq_medium.png?raw=true)

## Metadata Extension Tool to Annotate
## Microbiome Experiments for Qiita

A python tool to extend an existing tabular metadata file by inferring and adding 
the standard annotation columns required for submission to [Qiita](https://qiita.ucsd.edu/) and [EBI](https://www.ebi.ac.uk/).

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
