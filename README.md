# dp_tools

A collection of tools used for data processing workflows used for my work at NASA GeneLab

[Current documentation](https://torres-alexis.github.io/dp_tools/dp_tools.html) (hosted on github pages)

## Installation

```bash
pip install -e .
```

Or from git: `pip install git+https://github.com/torres-alexis/dp_tools.git`

### System dependencies (optional)

`dpt osd download-files` uses **GNU parallel** and **curl** for faster parallel downloads when both are on `PATH`. If either is missing, it falls back to threaded downloads via Python `requests`.

```bash
# conda / mamba
conda install -c conda-forge parallel curl

# Debian / Ubuntu
sudo apt install parallel curl
```

## Command-Line Tools

The dp_tools package provides command-line tools for working with OSDR datasets.

**Primary interface:** `dp_tools` and `dpt` are equivalent entry points.

**Standalone commands:** `dpt-get-isa-archive`, `dpt-isa-to-runsheet`

### ISA Archive Management

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

# Same via standalone command (downloads to current directory)
dpt-get-isa-archive --accession GLDS-194
```

#### Convert ISA archive (.zip) to Runsheet

```bash
dpt isa to-runsheet <accession> --config-type CONFIG_TYPE --config-version CONFIG_VERSION --isa-archive ISA_ARCHIVE [--output-dir OUTPUT_DIR]
```

Converts an ISA archive to a runsheet compatible with GeneLab processing workflows.

Supported `CONFIG_TYPE` values include:
* `bulkRNASeq`
* `microarray` (Generic DNA microarray, creates runsheets for any Agilent or Affymetrix assays found in the ISA archive)
* `microarray_agilent` (Specifically for Agilent 1-channel assays)
* `microarray_affymetrix` (Specifically for Affymetrix assays)
* `methylSeq`
* `metagenomics`
* `amplicon` (Generic amplicon, creates runsheets for any 16S, 18S, or ITS assays found in the ISA archive)
* `amplicon_16s` (Specifically for 16S assays)
* `amplicon_its` (Specifically for ITS assays)
* `amplicon_18s` (Specifically for 18S assays)

**Examples:**

Generate bulkRNAseq runsheet from ISA archive:
```bash
# Note: dpt isa get GLDS-194 downloads OSD-194_metadata_OSD-194-ISA.zip (GLDS maps to OSD)
dpt isa get GLDS-194
dpt isa to-runsheet GLDS-194 --config-type bulkRNASeq --config-version Latest --isa-archive OSD-194_metadata_OSD-194-ISA.zip
```

Same via standalone command:
```bash
dpt-isa-to-runsheet --accession GLDS-194 --config-type bulkRNASeq --config-version Latest --isa-archive OSD-194_metadata_OSD-194-ISA.zip
```

Generate 16S amplicon runsheet from ISA archive:
```bash
dpt isa to-runsheet OSD-694 --config-type amplicon_16s --isa-archive OSD-694_metadata_OSD-694-ISA.zip
# Output: OSD-694_amplicon_16S_v1_runsheet.csv
```

Generate all relevant amplicon runsheets from ISA archive:
```bash
dpt isa to-runsheet OSD-694 --config-type amplicon --isa-archive OSD-694_metadata_OSD-694-ISA.zip
# Example outputs if both 16S and ITS are present:
# OSD-694_amplicon_16S_v1_runsheet.csv
# OSD-694_amplicon_ITS_v1_runsheet.csv
```

### OSD API Interaction

The `dpt osd` commands provide functionality for interacting with the Open Science Data Repository (OSDR) API:

#### Download Files

```bash
dpt osd download-files <osd-id> [FILE-PATTERN] [OPTIONS]
```

**Selection modes** (use one primary mode):

| Mode | How |
|------|-----|
| Glob pattern | Positional `FILE-PATTERN` (`*`, `?`) |
| ISA assay table | `-a` / `--isa-assay` + optional `--data-file-column` (default: `Raw Data File`) |
| Repository category | `-c` / `--category` + optional `--subcategory` (see `--list-categories`) |

`-a` cannot be combined with `FILE-PATTERN` or `-c`. With `-c`, an optional `FILE-PATTERN` further narrows by filename.

**Category filtering:** `-c` and `--subcategory` are repeatable. Multiple values are OR'd within each level (e.g. two `--subcategory` flags = files in subcategory A **or** B). Omit `--subcategory` to include all subcategories under the selected `-c` value(s).

**Other options:** `--dry-run`, `-y` (no prompt), `-o` output dir, `-j` parallel jobs. Uses GNU `parallel` + `curl` when available; otherwise threaded downloads.

**Examples:**

List the OSDR file hierarchy (matches the repository browser):
```bash
dpt osd download-files OSD-240 --list-categories
```

By category:
```bash
# All files in a top-level category
dpt osd download-files OSD-240 -c "RNA-Seq" --dry-run

# One subcategory
dpt osd download-files OSD-240 -c "RNA-Seq" --subcategory "Raw sequence data" --y

# Multiple subcategories (OR)
dpt osd download-files OSD-240 -c "GeneLab Processed RNA-Seq Files" \
  --subcategory "Raw counts data" \
  --subcategory "Normalized counts data" \
  --dry-run

# Multiple categories (OR)
dpt osd download-files OSD-240 -c "Study Metadata Files" -c "RNA-Seq" --dry-run
```

From ISA assay table:
```bash
dpt isa get OSD-240
dpt osd download-files OSD-240 -a a_OSD-240_transcription-profiling_rna-sequencing-(rna-seq)_illumina.txt

# Different column name, if needed
dpt osd download-files OSD-240 -a a_OSD-240_....txt --data-file-column "Derived Data File"
```

By glob pattern:
```bash
dpt osd download-files OSD-237 "*raw.fastq.gz"
dpt osd download-files OSD-194 "*tar" -o ./data -j 10 --y
```

#### Get Sample Names

```bash
dpt osd get-samples <osd-id> [--output OUTPUT] [-t TABLE_INDEX] [-i]
```

Downloads the study's ISA archive from OSDR, then reads the `Sample Name` column from a single ISA table inside the zip. If exactly one assay table (`a_*`) is present, it is used automatically. If multiple tables exist, lists indices and exits; pass `--table-index` / `-t` (cluster-safe) or `-i` / `--interactive` to choose at a prompt.

**Examples:**
```bash
dpt osd get-samples OSD-194

# Multiple tables: lists options, then re-run with index
dpt osd get-samples OSD-694 --table-index 0

dpt osd get-samples OSD-194 --output my_samples.txt

# Interactive selection (local use)
dpt osd get-samples OSD-694 -i
```

<!--

Validation and Verification

The `dpt validation` commands provide functionality for validating data processing outputs:

```bash
dpt validation run PLUGIN_DIR DATA_DIR RUNSHEET_PATH [OPTIONS]
dpt validation manual-checks VALIDATION_REPORT
dpt validation spec PLUGIN_DIR DATA_DIR RUNSHEET_PATH [OPTIONS]
```

* `PLUGIN_DIR`: Path to validation plugin (e.g. `dp_tools__bulkRNASeq`). Use `dpt validation run --help` for options.
* `DATA_DIR`: Root directory of processed dataset.
* `RUNSHEET_PATH`: Path to runsheet CSV.

-->

For more detailed information on all commands, use the `--help` option:

```bash
dpt --help
dpt isa --help
dpt osd --help
# Standalone commands
dpt-get-isa-archive --help
dpt-isa-to-runsheet --help
```
