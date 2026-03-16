# dp_tools

A collection of tools used for data processing workflows used for my work at NASA GeneLab

[Current documentation](https://torres-alexis.github.io/dp_tools/dp_tools.html) (hosted on github pages)

## Installation

```bash
pip install -e .
```

Or from git: `pip install git+https://github.com/torres-alexis/dp_tools.git`

## Command-Line Tools

The dp_tools package provides command-line tools for data processing workflows.

**Primary interface:** `dp_tools` or `dpt` alias.

**Standalone commands** (same functionality):
* `dpt-get-isa-archive` — download ISA archive
* `dpt-isa-to-runsheet` — convert ISA archive to runsheet

### ISA Archive Management

The `dpt isa` commands provide functionality for working with Investigation-Study-Assay (ISA) archives:

#### Get ISA Archive

```bash
dpt isa get <accession> [--output-dir OUTPUT_DIR]
```

Downloads an ISA archive for a given GLDS or OSD accession number.

**Examples:**
```bash
# Download ISA archive for GLDS-194
dpt isa get GLDS-194

# Download ISA archive for OSD-194 to a specific directory
dpt isa get OSD-194 --output-dir /path/to/output
```

#### Convert ISA archive (.zip) to Runsheet

```bash
dpt isa to-runsheet <accession> --config-type CONFIG_TYPE --config-version CONFIG_VERSION --isa-archive ISA_ARCHIVE [--output-dir OUTPUT_DIR]
```

Converts an ISA archive to a runsheet compatible with GeneLab processing workflows.

Supported `CONFIG_TYPE` values include:
* `bulkRNASeq`
* `methylSeq`
* `metagenomics`
* `amplicon` (Generic amplicon, creates runsheets for any 16S, 18S, or ITS assays found in the ISA archive)
* `amplicon_16s` (Specifically for 16S assays)
* `amplicon_its` (Specifically for ITS assays)
* `amplicon_18s` (Specifically for 18S assays)

**Examples:**
```bash
# Convert ISA archive to a bulkRNASeq runsheet
# Note: dpt isa get GLDS-194 downloads OSD-194_metadata_OSD-194-ISA.zip (GLDS maps to OSD)
dpt isa get GLDS-194
dpt isa to-runsheet GLDS-194 --config-type bulkRNASeq --config-version Latest --isa-archive OSD-194_metadata_OSD-194-ISA.zip

# Convert ISA archive targeting only 16S amplicon assays
dpt isa to-runsheet OSD-694 --config-type amplicon_16s --isa-archive OSD-694_metadata_OSD-694-ISA.zip
# Output: OSD-694_amplicon_16S_v1_runsheet.csv

# Convert ISA archive using the generic amplicon config
# This will find all amplicon assays (16S, 18S, ITS) and create separate runsheets
dpt isa to-runsheet OSD-694 --config-type amplicon --isa-archive OSD-694_metadata_OSD-694-ISA.zip
# Example Outputs (if both 16S and ITS assays are present):
# OSD-694_amplicon_16S_v1_runsheet.csv
# OSD-694_amplicon_ITS_v1_runsheet.csv
```

### OSD API Interaction

The `dpt osd` commands provide functionality for interacting with the Open Science Data Repository (OSDR) API:

#### Download Files

```bash
dpt osd download-files <osd-id> <file-pattern> [--dry-run] [--y]
```

Downloads files from OSDR that match a specified pattern.

**Examples:**
```bash
# Download files matching a pattern (regex)
# Note: raw fastq may be inside .tar archives for some datasets; use .*\.tar$ for those
dpt osd download-files OSD-194 ".*\.fastq\.gz$"

# Just list the URLs without downloading (dry run)
dpt osd download-files OSD-194 ".*\.fastq\.gz$" --dry-run

# Download without interactive prompts
dpt osd download-files OSD-194 ".*\.fastq\.gz$" --y
```

#### Get Sample Names

```bash
dpt osd get-samples <osd-id> [--output OUTPUT]
```

Extracts sample names from an OSD accession's ISA archive and saves them to a file.
When only one assay file is found, it's automatically selected without prompting.

**Examples:**
```bash
# Get sample names for OSD-194 and save to the default file (samples.txt)
dpt osd get-samples OSD-194

# Get sample names and save to a specific file
dpt osd get-samples OSD-194 --output my_samples.txt
```

### Validation and Verification

The `dpt validation` commands provide functionality for validating data processing outputs:

```bash
dpt validation run PLUGIN_DIR DATA_DIR RUNSHEET_PATH [OPTIONS]
dpt validation manual-checks VALIDATION_REPORT
dpt validation spec PLUGIN_DIR DATA_DIR RUNSHEET_PATH [OPTIONS]
```

* `PLUGIN_DIR`: Path to validation plugin (e.g. `dp_tools__bulkRNASeq`). Use `dpt validation run --help` for options.
* `DATA_DIR`: Root directory of processed dataset.
* `RUNSHEET_PATH`: Path to runsheet CSV.

For more detailed information on all commands, use the `--help` option:

```bash
dpt --help
dpt isa --help
dpt osd --help
dpt validation --help
# Standalone commands
dpt-get-isa-archive --help
dpt-isa-to-runsheet --help
```
