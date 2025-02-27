# dp_tools

A streamlined toolkit for data processing validation and verification with a focus on simplicity and usability.

## Overview

dp_tools provides modular validation utilities for bioinformatics pipelines, with a focus on:

- **Simplicity**: Minimal abstractions and clear execution flow
- **Modularity**: Independent components that can be used separately
- **Configuration-driven**: Behavior controlled via configuration files rather than code
- **Well-documented**: Comprehensive docstrings and examples

## Structure

```
dp_tools/
├── core/                     # Core functionality
│   └── utils/                # Core utilities
│       ├── file_utils.py     # File handling utilities
│       ├── multiqc.py        # MultiQC parsing utilities
│       ├── logging.py        # Logging functionality
│       └── mover.py          # File organization utilities
├── api/                      # API integrations
│   ├── isa.py                # ISA archive and runsheet functions
│   └── GLOpenAPI.py          # GLOpenAPI utilities (soon)
├── assays/                   # Assay-specific code
│   ├── rnaseq/               # RNA-seq specific validation
│   │   ├── raw_reads.py      # Raw reads structure/validation
│   │   ├── trimmed_reads.py  # Trimmed reads structure/validation
│   │   ├── alignment.py      # Alignment structure/validation
│   │   └── rseqc.py          # RSeQC structure/validation
│   └── scrna/                # scRNA-seq specific validation
│       └── ...               # scRNA-seq validation modules
├── cli/                      # Command line interfaces
└── config/                   # Configuration files
    └── rnaseq/               # RNA-seq specific configs
```

## Key Features

- Simple validation framework with validation functions organized by assay type
- File integrity checks for common bioinformatics formats
- Statistical validation for pipeline outputs
- MultiQC integration for report validation
- Support for multiple assay types
- File organization based on component-specific structure definitions

## Installation

```bash
# Install from PyPI
pip install dp_tools

# Development installation
git clone https://github.com/torres-alexis/dp_tools.git
cd dp_tools
pip install -e .
```

## Docker

You can also use dp_tools with Docker:

```bash
# Build the Docker image
docker build -t dp_tools .

# Run a command using the container
docker run --rm dp_tools dpt mover --help

# Mount your data directory to process files
docker run --rm -v /path/to/data:/data dp_tools dpt mover \
    --assay rnaseq \
    --component raw_reads \
    --outdir /data/results \
    -i raw_fastq:/data/input/fastq/ \
    -i raw_fastqc:/data/input/fastqc/
```

## Usage

### File Organization with the Mover

The mover tool organizes files according to assay-specific and component-specific layouts:

```bash
# Organize raw reads files
dpt mover --assay rnaseq --component raw_reads --outdir ./results \
    -i raw_fastq:./path/to/fastq/ \
    -i raw_fastqc:./path/to/fastqc/ \
    -i raw_multiqc:./path/to/multiqc/

# Use with glob patterns
dpt mover --assay rnaseq --component raw_reads --outdir ./results \
    -i raw_fastq:"./*.fastq.gz" \
    -i raw_fastqc:"./*_fastqc.*"
    
# Dry run to preview changes
dpt mover --assay rnaseq --component raw_reads --outdir ./results \
    -i raw_fastq:./path/to/fastq/ --dry-run
```

Each component defines its own output structure in a `STRUCTURE` dictionary, for example:

```python
# From dp_tools/assays/rnaseq/raw_reads.py
STRUCTURE = {
    "rnaseq": {
        "microbes": {
            "components": {
                "raw_reads": {
                    "outputs": {
                        "raw_fastq": "00-RawData/Fastq",
                        "raw_fastqc": "00-RawData/FastQC_Reports",
                        "raw_multiqc": "00-RawData/FastQC_Reports"
                    }
                }
            }
        }
    }
}
```

### Validation

Basic validation:

```python
from dp_tools.assays.rnaseq.raw_reads import validate_raw_reads
from dp_tools.assays.rnaseq.alignment import validate_alignments

# Validate raw reads
raw_results = validate_raw_reads(
    outdir="path/to/output",
    samples_txt="path/to/samples.txt",
    paired_end=True
)

# Validate alignments
align_results = validate_alignments(
    outdir="path/to/output",
    samples_txt="path/to/samples.txt",
    paired_end=True
)
```

## License

[MIT License](LICENSE)