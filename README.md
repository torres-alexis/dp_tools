# dp_tools

A collection of tools used for data processing workflows used for my work at NASA GeneLab.

[Current documentation](https://torres-alexis.github.io/dp_tools/dp_tools.html)

## Installation

Python **3.11+** required.

### Release

**pip:**

```bash
pip install git+https://github.com/torres-alexis/dp_tools.git@v1.3.12
```

**Conda environment:**

```bash
curl -LO https://raw.githubusercontent.com/torres-alexis/dp_tools/v1.3.12/condaEnv.yaml
conda env create -f condaEnv.yaml
conda activate dp_tools
```

Optional for pip only: `parallel` and `curl` on PATH speed up `dpt osd download-files` (`apt install parallel curl` or `conda install -c conda-forge parallel curl`). Otherwise downloads use threaded Python `requests`.

### Development

```bash
git clone https://github.com/torres-alexis/dp_tools.git
cd dp_tools
pip install -e .
```

### Container

`quay.io/nasa_genelab/dp_tools:latest`

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
* `microarray` (auto-detects Agilent or Affymetrix assays in the ISA archive)
* `microarray_agilent` (Agilent 1-channel assays)
* `microarray_affymetrix` (Affymetrix assays)
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

**Other options:** `--dry-run`, `-y` (no prompt), `-o` output dir, `-j` parallel jobs. Uses GNU `parallel` + `curl` when available; otherwise threaded Python `requests`.

**Examples:**

List the OSDR file hierarchy:
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

Downloads the study's ISA archive from OSDR, then reads the `Sample Name` column from a single ISA table inside the zip. If exactly one assay table (`a_*`) is present, it is used automatically. If multiple tables exist, lists indices and exits; pass `--table-index` / `-t` or `-i` / `--interactive` to choose a table.

**Examples:**
```bash
dpt osd get-samples OSD-194

# Multiple tables: lists options, then re-run with index
dpt osd get-samples OSD-694 --table-index 0

dpt osd get-samples OSD-194 --output my_samples.txt

# Interactive table selection
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
