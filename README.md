# dp_tools

Data processing tools

## Install

```bash
pip install -e .
```

## Commands

### Download ISA Archive

```bash
dp_tools get-isa --accession OSD-185 --output-dir ./data
```

**Parameter Definitions:**
- `--accession` - Dataset accession ID (OSD or GLDS format. Must use --plugin if using GLDS)
- `--output-dir` - Directory to save downloaded files (default: current directory)
- `--force` - Overwrite existing files if they exist
- `--plugin` - Plugin directory containing config.yaml with API settings
- `--verbose` - Enable detailed output for debugging

**Input Data:**
- None (downloads from OSDR repository)

**Output Data:**
- ISA archive file (ZIP format)

### Organize Files

```bash
dp_tools move --plugin ./plugins/rnaseq --component raw_reads --outdir ./results \
    -i raw_fastq:./path/to/fastq/ -i raw_fastqc:./path/to/fastqc/
```

**Parameter Definitions:**
- `--plugin` - Directory containing plugin configuration
- `--component` - Component type (e.g., raw_reads, trimmed_reads)
- `--outdir` - Output directory for organized files
- `-i` - Input files in format 'key:path' (can use multiple times)
- `--use-symlinks` - Create symbolic links instead of copying (default)
- `--dry-run` - Preview changes without making them

**Input Data:**
- Source files specified by -i parameters
- Plugin directory with structure definitions

**Output Data:**
- Organized files/symlinks in output directory structure