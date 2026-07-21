# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.12]

### Added

- `dpt osd download-files`: parallel downloads (`-j`), output directory (`-o`)
- `dpt osd download-files`: ISA assay selection (`-a` / `--isa-assay`, `--data-file-column`; default `Raw Data File`)
- `dpt osd download-files`: OSDR category filtering (`-c` / `--category`, repeatable `--subcategory`, `--list-categories`)
- `dpt osd get-samples`: `--table-index` / `-t` for non-interactive ISA table selection; `-i` / `--interactive` for prompt
- `glds_api.commons`: `filter_filenames`, `filenames_from_isa_assay`, `format_file_hierarchy`
- Restored microarray runsheet configs (`microarray` with Agilent/Affymetrix auto-detection; explicit `microarray_agilent` / `microarray_affymetrix` keys) with platform-specific data assets and pandera schemas

### Changed

- README: expanded OSD/ISA CLI docs; validation section commented out; download examples split by selection mode; system deps (`curl`, GNU `parallel`) documented
- `dpt osd get-samples`: multiple ISA tables now list indices and exit; use `-t` / `--table-index` or `-i` / `--interactive` to choose
- `dpt isa convert`: silent alias for `to-runsheet` (deprecation warning removed)
- `dp_tools` and `dpt` documented as equivalent entry points
- MultiQC updated from 1.26 to 1.35
- Dependency minimums bumped (pandas 3.x, pandera 0.32.x, pytest 9.x, and others); dropped unused `importlib_metadata` (stdlib on Python 3.11+)
- Minimum Python version raised to **3.11** (`requires-python`; required by pandas 3.x)
- Dockerfile: `python:3.11-slim-bookworm` base (Debian bookworm); Python 3.11 from official image
- CI and Gitpod: Python 3.11

### Fixed

- README: `download-files` documented as glob patterns (not regex)
- Pandera import: use `pandera.pandas` to silence deprecation warning on CLI startup
- Dockerfile build failure on focal/deadsnakes; image now uses `python:3.11-slim-bookworm`

## [1.3.11]

### Added

- Installation instructions in README
- Standalone commands (`dpt-get-isa-archive`, `dpt-isa-to-runsheet`) documented in README alongside primary `dpt` interface

### Changed

- Standalone commands no longer emit deprecation warnings; treated as alternative interface
- README to-runsheet example: corrected ISA filename (GLDS→OSD mapping), added `dpt isa get` step
- README download-files example: added note about `.tar` vs `.fastq.gz` for different datasets
- GitHub Pages workflow: trigger on tag push only, added `configure-pages`, upgraded `upload-pages-artifact` to v4

### Fixed

- `dpt isa to-runsheet` with `--output-dir`: ISA archive path now resolved before chdir, so relative paths work correctly

## [1.3.10]

### Added

- Added v3 amplicon configs: Add Parameter Value[Raw Sequence Data] as a source for raw data files, remove unnecessary organism and raw data file suffixes columns


## [1.3.9]

### Fixed

- Fixed bulkRNASeq runsheet generation for legacy datasets that use the `Parameter Value[Merged Sequence Data]` field

## [1.3.8]

### Added

- Optional schema fields skip if not present in ISA archive for backwards compatibility
- Added the following two columns which are only used in RNAseq DGE processing
  - Added `Source Name` optional string column to bulkRNASeq runsheet generation 
  - Added `Has Tech Reps` optional boolean column to bulkRNASeq runsheet generation from assay table
    - When `Has Tech Reps` is present, requires `Source Name` column to also be present (dependency validation)


## [1.3.7]

### Added

- Added `amplicon_16s`, `amplicon_18s`, `amplicon_its` profiles for runsheet generation.

### Changed

- Removed host organism dependency for amplicon runsheet generation.
- Forced technology type (`16S`/`18S`/`ITS`) inclusion in all amplicon runsheet names.
- Removed assay table name inclusion from runsheet names for datasets with multiple amplicon assay tables (verified all current OSD datasets have at most one amplicon assay table per technology type).
- *Note: Support for combined '16S and ITS' assay type (OSD-249) is pending.*

### Fixed

- Dockerfile now sets the PATH environment variable using ENV.
- Resolved Docker build dependency conflict (PyYAML) using `--ignore-installed` during pip install.

## [1.3.6]

### Changed

- Enhanced GLDS API functionality to handle both OSD and GLDS accessions
- Fixed GLDS to OSD mapping to properly use search API instead of direct substitution
- Added support for cases where GLDS-### doesn't map directly to OSD-### (e.g., GLDS-570 → OSD-576)
- Updated dependencies to latest versions
- Modernized project structure with pyproject.toml
- Changed OSD get-samples command to automatically select assay file when only one is available

### Fixed

- importlib.resources usage updated to work with newer Python versions
- Fixed ISA download function to correctly handle both GLDS and OSD accessions with proper regex pattern matching
- Fixed column name reference in ISA download function from 'filename' to 'file_name' to match the DataFrame structure
- Improved remote URL handling for downloading ISA archives
- Missing parameters in check_model.py validation protocol
- Replaced logging with loguru consistently throughout codebase
- Improved test reliability with better mocking approach for file pattern matching

## [1.3.5]

### Changed

- dpt-isa-to-runsheet: Now creates multiple runsheets if more than 1 match found in ISA
- dpt-isa-to-runsheet: Minor robustness fix to factor column assertion
- dpt-isa-to-runsheet: Added support for amplicon and metagenomics
- post-processing: Changed assay update logic to append processed file columns rather than replace
- Added multiQC metrics extraction (rewrite in progress)

## [1.3.4]

### Changed

- Table updates (associated with updating ISA archive files) now separates multiple files in a field with ',' instead of ', '

## [1.3.3]

### Added

- Support for data asset key sets and run components in updated validation interface (i.e. by 'dpt validation')

## [1.3.2]

### Fixed

- Refactored ISA archive parsing functions as prior the fallback wasn't being used in all calls (specifically the plug in based ones)

## [1.3.1]

### Fixed

- Parsing for ISA Archives met 'ISO-8859-1' encoding but not 'utf-8'
  - Specifically, 'utf-8' is attempted and 'ISO-8859-1' is used as a fallback

## [1.3.0]

### Added

- Improved V&V interface
  - Plugin support for protocols & checks
  - Specification generation via `dpt validation spec`
  - Generalized validation runner via `dpt validation run`
  - Manual check interface via `dpt validation manual-checks`
  - CLI interface implemented using [click](https://click.palletsprojects.com/en/8.1.x/)

- Started to use logging via [loguru](https://github.com/Delgan/loguru)

### Removed

- Microarray specific protocols/checks (now implemented as plugins outside dp_tools)

## [1.2.1]

### Added

- Ability to inject columns during runsheet generation

## [1.2.0]

### Added

- Microarray (Agilent 1 Channel) V&V protocol
- Pandera as dependency for better validation tooling

### Changed

- BulkRNASeq runsheet validation enhanced
  - Upgraded from Schema to Pandera
  - Added checks for dataset metadata columns like 'paired_end'
  - Added sanity check for 'read2_path' column optional nature

### Fixed

- [#19](https://github.com/J-81/dp_tools/issues/19)

## [1.1.9]

### Added
- Runsheet generation for methlySeq ISA archives

## [1.1.8]

### Changed
- GLDS API usage now considers the 'OSD' accession ID as the study ID instead of 'GLDS'.  This is consistent with the recent release of the [OSDR](https://osdr.nasa.gov/bio/)
### Fixed
- Fixes incorrect numeric inferrence for strings (commit: 3b0d953)[https://github.com/J-81/dp_tools/commit/3b0d9537de73363aaa78979b78b3a209c69ccd45]

## [1.1.7]

### Fixed
- Fixes incorrect unit detection for runsheet generation [#14](https://github.com/J-81/dp_tools/issues/14)
## [1.1.6]

### Added
- Stdout logging for scripts, this better explains what is happening during the script

### Fixed
- Missing Microarray technology valid combination and handling of multiple valid combinations
## [1.1.5]

### Fixed
- Staging runsheets failing to extract unit columns 
- V&V crash related to factor columns being inferred as numeric. Now correctly inferring as string values.

## [1.1.4]
### Added
- Integrity check for gzipped files to bulkRNASeq checks and protocol

### Changed
- Pinned Pandas version to 1.4.4 (prior: no pin, most recent version installed)
  - Version 1.5 causes changes to checksum for pandas objects and would require updating all tests that include a checksum (planned for future)
## [1.1.3]

### Fixed
- Fixing false V&V halt flagging: Add in micro sign as whitelisted (better in sync with r make.names function)
- Expected location of SampleTable.csv and ERCC_SampleTable.csv in 
## [1.1.2]

### Fixed
- Fixing false V&V halt flagging: Add in greek characters as whitelisted (better in sync with r make.names function)

## [1.1.1]
### Fixed
- Incorrect detection of has_ERCC from ISA Archives
  - Example Impacted GLDS: 161,162,163,173
- Runsheet generation failing for different variations of raw reads data column names
  - Example Impacted GLDS: 105,138

## 1.1.0

### First Production Release
- Prior 1.0.0 tagged versions were actually develop style releases
- Moving ahead only production releases will have tags without 'rc' (release candidate) in the name

### Quality Updates
- Various flag messages improved
- Documentation updated

### Added
- Check related to multiQC samples inclusion

## 1.0.8rc

### Changed

- Updated GeneLab filename to url mapping to utilize the [GeneLab public API](https://genelab.nasa.gov/genelabAPIs)
  - Addresses removal of prior-used deprecated endpoints

## 1.0.7rc

### Dockerfile

- Added samtools as needed for certain checks

### Checks

#### Fixed

- check_contrasts_table_rows: message no longer introduces extra newlines into log

## Planned

## rc1.0.6

### Added

#### BulkRNASeq V&V Reporting

- A validation protocol that runs on a BulkRNASeq dataset model
- Includes generation of report files

#### BulkRNASeq Data Model From Nextflow RNASeq Concensus Pipeline

- A set of multi-stage loaders to create a data model
- Includes: validation system and multiQC powered data extraction

### Fixed

- Tilde characters are not converted to periods in contrasts: https://tower.nf/orgs/GL_Testing_Nextflow/workspaces/Nextflow_RCP_Testing/watch/1t8TfGbpDmCVNK
  - this should emulate R make.names behaviour completely

#### BulkRNASeq Reporter File Generation

- Data assets tagged with file categories for reporter file export including:
  - md5sum table
  - curation tables [GeneLab internal use]

[1.1.1]: https://github.com/j-81/dp_tools/compare/1.1.0...1.1.1
[1.1.2]: https://github.com/j-81/dp_tools/compare/1.1.1...1.1.2
[1.1.3]: https://github.com/j-81/dp_tools/compare/1.1.2...1.1.3
[1.1.4]: https://github.com/j-81/dp_tools/compare/1.1.3...1.1.4
[1.1.5]: https://github.com/j-81/dp_tools/compare/1.1.4...1.1.5
[1.1.6]: https://github.com/j-81/dp_tools/compare/1.1.5...1.1.6
[1.1.7]: https://github.com/j-81/dp_tools/compare/1.1.6...1.1.7
[1.1.8]: https://github.com/j-81/dp_tools/compare/1.1.7...1.1.8
[1.1.9]: https://github.com/j-81/dp_tools/compare/1.1.8...1.1.9
[1.2.0]: https://github.com/j-81/dp_tools/compare/1.1.9...1.2.0
[1.2.1]: https://github.com/j-81/dp_tools/compare/1.2.0...1.2.1
[1.3.0]: https://github.com/j-81/dp_tools/compare/1.2.1...1.3.0
[1.3.1]: https://github.com/j-81/dp_tools/compare/1.3.0...1.3.1
[1.3.2]: https://github.com/j-81/dp_tools/compare/1.3.1...1.3.2
[1.3.3]: https://github.com/j-81/dp_tools/compare/1.3.2...1.3.3
[1.3.4]: https://github.com/j-81/dp_tools/compare/1.3.3...1.3.4
[1.3.5]: https://github.com/torres-alexis/dp_tools/compare/1.3.4...1.3.5
[1.3.6]: https://github.com/torres-alexis/dp_tools/compare/1.3.5...1.3.6
[1.37]: https://github.com/torres-alexis/dp_tools/compare/1.3.6...1.3.7
[1.38]: https://github.com/torres-alexis/dp_tools/compare/1.3.7...1.3.8
