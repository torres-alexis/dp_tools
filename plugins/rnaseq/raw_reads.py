"""
RNA-seq raw reads structure definition and validation.
"""
from pathlib import Path
import re
import os
import glob
import gzip
import subprocess
import json
from io import TextIOWrapper
from typing import Dict, List, Optional, Union, Tuple, Any
import pandas as pd
import zipfile
import statistics
import numpy as np

from dp_tools.utils.logging import Status, ValidationResult, ValidationLogger
from dp_tools.utils.file_utils import get_paired_end_from_runsheet

# Output structure config - only includes what's needed for raw_reads validation
STRUCTURE = {
    "outputs": {
        "raw_fastq": "00-RawData/Fastq",
        "raw_fastqc": "00-RawData/FastQC_Reports",
        "raw_multiqc": "00-RawData/FastQC_Reports"
    }
}

def validate_raw_reads(
    outdir: Path,
    runsheet: Path,
    assay_suffix: str = "_GLbulkRNAseq",
    logger: Optional[ValidationLogger] = None
) -> Dict[str, ValidationResult]:
    """
    Validate raw RNA-seq reads files.
    
    Args:
        outdir: Path to the output directory containing raw data
        runsheet: Path to the runsheet CSV file
        assay_suffix: Suffix for assay files
        logger: Optional ValidationLogger for logging results
    
    Returns:
        Dictionary of validation results
    """
    # Initialize logger if not provided
    if logger is None:
        logger = ValidationLogger()
    
    results = {}
    
    # Get paired_end status from runsheet
    try:
        paired_end = get_paired_end_from_runsheet(runsheet)
        logger.log(
            "raw_reads", 
            "global", 
            "paired_end_detection", 
            ValidationResult(
                status=Status.GREEN,
                message=f"Determined {'paired-end' if paired_end else 'single-end'} data from runsheet",
                details={"paired_end": paired_end}
            )
        )
    except ValueError as e:
        # If we can't determine from runsheet, this is a critical failure
        result = ValidationResult(
            status=Status.HALT,
            message=f"Failed to determine paired-end status from runsheet: {str(e)}",
            details={"error": str(e)}
        )
        logger.log("raw_reads", "global", "paired_end_detection", result)
        results["paired_end_detection"] = result
        return results
    
    # Load sample IDs from runsheet
    sample_ids = _load_sample_ids(runsheet)
    if not sample_ids:
        result = ValidationResult(
            status=Status.HALT,
            message="No sample IDs found in runsheet",
            details={"runsheet": str(runsheet)}
        )
        logger.log("raw_reads", "global", "sample_ids", result)
        results["sample_ids"] = result
        return results
    
    # Get expected file paths
    structure = STRUCTURE["outputs"]
    raw_fastq_dir = outdir / structure["raw_fastq"]
    raw_fastqc_dir = outdir / structure["raw_fastqc"]
    
    # Check if directories exist
    if not raw_fastq_dir.exists():
        result = ValidationResult(
            status=Status.HALT, 
            message=f"Raw FASTQ directory not found: {raw_fastq_dir}",
            details={"expected_path": str(raw_fastq_dir)}
        )
        logger.log("raw_reads", "global", "raw_fastq_dir", result)
        results["raw_fastq_dir"] = result
        return results
    
    # Track read counts for all samples
    read_counts = {}
    
    # Validate FASTQ files for each sample
    for sample_id in sample_ids:
        # Generate expected file patterns
        expected_files = _get_expected_files(sample_id, paired_end, assay_suffix)
        
        # Check FASTQ files existence
        fastq_result = _validate_fastq_files(raw_fastq_dir, expected_files)
        logger.log("raw_reads", sample_id, "fastq_files", fastq_result)
        results[f"{sample_id}_fastq"] = fastq_result
        
        # If files exist, perform integrity and format checks
        if fastq_result.status == Status.GREEN:
            # Check GZIP integrity
            for key, files in fastq_result.details.get("files", {}).items():
                for file_name in files:
                    gzip_result = _check_gzip_integrity(raw_fastq_dir / file_name)
                    logger.log("raw_reads", sample_id, f"gzip_integrity_{file_name}", gzip_result)
                    results[f"{sample_id}_gzip_{file_name}"] = gzip_result
                    
                    # Check FASTQ format if GZIP integrity passed
                    if gzip_result.status == Status.GREEN:
                        fastq_format_result, read_count = _check_fastq_format(raw_fastq_dir / file_name)
                        logger.log("raw_reads", sample_id, f"fastq_format_{file_name}", fastq_format_result)
                        results[f"{sample_id}_format_{file_name}"] = fastq_format_result
                        
                        # Store read count for paired-end comparison
                        if paired_end:
                            read_counts[(sample_id, file_name)] = read_count
        
        # Check FastQC files if directory exists
        if raw_fastqc_dir.exists():
            fastqc_result = _validate_fastqc_files(raw_fastqc_dir, expected_files)
            logger.log("raw_reads", sample_id, "fastqc_files", fastqc_result)
            results[f"{sample_id}_fastqc"] = fastqc_result
    
    # For paired-end data, verify read counts match between R1 and R2
    if paired_end:
        for sample_id in sample_ids:
            r1_key = None
            r2_key = None
            for key, count in read_counts.items():
                if key[0] == sample_id:
                    if "_R1_" in key[1]:
                        r1_key = key
                    elif "_R2_" in key[1]:
                        r2_key = key
            
            if r1_key and r2_key:
                r1_count = read_counts[r1_key]
                r2_count = read_counts[r2_key]
                
                paired_result = _check_paired_counts(r1_key[1], r2_key[1], r1_count, r2_count)
                logger.log("raw_reads", sample_id, "paired_read_counts", paired_result)
                results[f"{sample_id}_paired_counts"] = paired_result
    
    # Validate MultiQC report if directory exists
    multiqc_dir = outdir / structure["raw_multiqc"]
    if multiqc_dir.exists():
        multiqc_result = _validate_multiqc(multiqc_dir)
        logger.log("raw_reads", "global", "multiqc", multiqc_result)
        results["multiqc"] = multiqc_result
        
        # Parse MultiQC if it was found and extract metrics
        if multiqc_result.status == Status.GREEN and "report" in multiqc_result.details:
            multiqc_path = multiqc_result.details["report"]
            try:
                metrics = _parse_multiqc_metrics(Path(multiqc_path), sample_ids, paired_end)
                
                # Log metrics for each sample
                for sample_id, sample_metrics in metrics.items():
                    logger.log(
                        "raw_reads", 
                        sample_id, 
                        "multiqc_metrics", 
                        ValidationResult(
                            status=Status.GREEN,
                            message=f"Extracted QC metrics for {sample_id}",
                            details={"metrics": sample_metrics}
                        )
                    )
                
                # Check for outliers in key metrics
                outlier_result = _detect_outliers(metrics)
                if outlier_result:
                    logger.log("raw_reads", "global", "outlier_detection", outlier_result)
                    results["outlier_detection"] = outlier_result
            except Exception as e:
                logger.log(
                    "raw_reads",
                    "global",
                    "multiqc_parsing",
                    ValidationResult(
                        status=Status.YELLOW,
                        message=f"Failed to parse MultiQC metrics: {str(e)}",
                        details={"error": str(e)}
                    )
                )
    
    return results


def _load_sample_ids(runsheet_path: Path) -> List[str]:
    """
    Load sample IDs from runsheet.
    
    Args:
        runsheet_path: Path to runsheet CSV
        
    Returns:
        List of sample IDs
    """
    try:
        # Try to load as CSV
        df = pd.read_csv(runsheet_path)
        
        # Look for common sample ID column names
        sample_col = None
        possible_columns = ["Sample Name", "sample_name", "Sample_Name", "SampleName", "sample name", "sample"]
        
        for col in possible_columns:
            if col in df.columns:
                sample_col = col
                break
        
        if sample_col:
            return df[sample_col].unique().tolist()
        else:
            # If no sample column found, check if it's a plain text file
            with open(runsheet_path, 'r') as f:
                lines = f.readlines()
                if len(lines) > 0 and ',' not in lines[0]:
                    # Assume it's a plain text file with one sample ID per line
                    return [line.strip() for line in lines if line.strip()]
            
            return []
            
    except Exception as e:
        print(f"Error loading runsheet: {e}")
        return []


def _get_expected_files(sample_id: str, paired_end: bool, assay_suffix: str = "") -> Dict[str, str]:
    """
    Get expected file patterns for a sample.
    
    Args:
        sample_id: Sample ID
        paired_end: Whether data is paired-end
        assay_suffix: Suffix for assay files (not currently used in file patterns)
        
    Returns:
        Dictionary of expected file patterns
    """
    expected = {}
    
    # FASTQ file patterns
    if paired_end:
        expected["fastq_r1"] = f"{sample_id}_R1_raw.fastq.gz"
        expected["fastq_r2"] = f"{sample_id}_R2_raw.fastq.gz"
    else:
        expected["fastq"] = f"{sample_id}_raw.fastq.gz"
    
    # FastQC file patterns
    if paired_end:
        expected["fastqc_r1_html"] = f"{sample_id}_R1_raw_fastqc.html"
        expected["fastqc_r2_html"] = f"{sample_id}_R2_raw_fastqc.html"
        expected["fastqc_r1_zip"] = f"{sample_id}_R1_raw_fastqc.zip"
        expected["fastqc_r2_zip"] = f"{sample_id}_R2_raw_fastqc.zip"
    else:
        expected["fastqc_html"] = f"{sample_id}_raw_fastqc.html"
        expected["fastqc_zip"] = f"{sample_id}_raw_fastqc.zip"
    
    return expected


def _validate_fastq_files(fastq_dir: Path, expected_files: Dict[str, str]) -> ValidationResult:
    """
    Validate FASTQ files for a sample.
    
    Args:
        fastq_dir: Directory containing FASTQ files
        expected_files: Dictionary of expected file patterns
        
    Returns:
        ValidationResult
    """
    missing_files = []
    found_files = {}
    
    # Check ONLY for FASTQ files (not FastQC files)
    relevant_keys = [key for key in expected_files if key.startswith("fastq") and not key.startswith("fastqc")]
    
    for key in relevant_keys:
        expected_file = expected_files[key]
        file_path = fastq_dir / expected_file
        
        if not file_path.exists():
            missing_files.append(expected_file)
        else:
            if key not in found_files:
                found_files[key] = []
            found_files[key].append(str(file_path.name))
    
    if missing_files:
        return ValidationResult(
            status=Status.RED,
            message=f"Missing FASTQ files: {', '.join(missing_files)}",
            details={"missing": missing_files, "found": found_files}
        )
    
    return ValidationResult(
        status=Status.GREEN,
        message="All expected FASTQ files found",
        details={"files": found_files}
    )


def _check_gzip_integrity(fastq_path: Path) -> ValidationResult:
    """
    Verify GZIP integrity of a FASTQ file.
    
    Args:
        fastq_path: Path to FASTQ.GZ file
        
    Returns:
        ValidationResult with status and details
    """
    try:
        # Run gzip -t to test integrity of the gzip file
        result = subprocess.run(
            ["gzip", "-t", str(fastq_path)], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            check=False  # Don't raise exception on non-zero return code
        )
        
        if result.returncode == 0:
            return ValidationResult(
                status=Status.GREEN,
                message=f"GZIP integrity check passed for {fastq_path.name}",
                details={"path": str(fastq_path)}
            )
        else:
            return ValidationResult(
                status=Status.RED,
                message=f"GZIP integrity check failed for {fastq_path.name}",
                details={
                    "path": str(fastq_path),
                    "error": result.stderr.decode('utf-8', errors='replace')
                }
            )
    except Exception as e:
        return ValidationResult(
            status=Status.RED,
            message=f"Error checking GZIP integrity for {fastq_path.name}",
            details={"path": str(fastq_path), "error": str(e)}
        )


def _check_fastq_format(fastq_path: Path, max_lines: int = 400000) -> Tuple[ValidationResult, int]:
    """
    Check if a FASTQ file has valid format.
    
    Args:
        fastq_path: Path to FASTQ.GZ file
        max_lines: Maximum number of lines to check
        
    Returns:
        Tuple of (ValidationResult, read_count)
    """
    try:
        invalid_header_lines = []
        read_count = 0
        
        with gzip.open(fastq_path, 'rt') as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                    
                # Check header lines (every 4th line, starting at 0)
                if i % 4 == 0:
                    read_count += 1
                    if not line.startswith('@'):
                        invalid_header_lines.append(i + 1)  # 1-indexed line numbers
                        if len(invalid_header_lines) >= 10:  # Stop after 10 errors
                            break
        
        if invalid_header_lines:
            return ValidationResult(
                status=Status.RED,
                message=f"Invalid FASTQ format in {fastq_path.name}: {len(invalid_header_lines)} header issues",
                details={
                    "path": str(fastq_path),
                    "invalid_lines": invalid_header_lines,
                    "read_count": read_count
                }
            ), read_count
        
        return ValidationResult(
            status=Status.GREEN,
            message=f"FASTQ format check passed for {fastq_path.name}",
            details={"path": str(fastq_path), "read_count": read_count}
        ), read_count
    
    except Exception as e:
        return ValidationResult(
            status=Status.RED,
            message=f"Error checking FASTQ format for {fastq_path.name}",
            details={"path": str(fastq_path), "error": str(e)}
        ), 0


def _check_paired_counts(r1_file: str, r2_file: str, r1_count: int, r2_count: int) -> ValidationResult:
    """
    Check that paired-end read counts match.
    
    Args:
        r1_file: Name of R1 file
        r2_file: Name of R2 file
        r1_count: Read count for R1
        r2_count: Read count for R2
        
    Returns:
        ValidationResult with status and details
    """
    if r1_count == r2_count:
        return ValidationResult(
            status=Status.GREEN,
            message=f"Paired read counts match ({r1_count} reads in each file)",
            details={
                "r1_file": r1_file,
                "r2_file": r2_file,
                "r1_count": r1_count,
                "r2_count": r2_count
            }
        )
    else:
        # Calculate percent difference
        diff = abs(r1_count - r2_count)
        diff_pct = (diff / max(r1_count, r2_count)) * 100
        
        status = Status.RED if diff_pct > 0.1 else Status.YELLOW
        
        return ValidationResult(
            status=status,
            message=(
                f"Paired read counts don't match: {r1_count} vs {r2_count} reads "
                f"({diff} difference, {diff_pct:.2f}%)"
            ),
            details={
                "r1_file": r1_file,
                "r2_file": r2_file,
                "r1_count": r1_count,
                "r2_count": r2_count,
                "difference": diff,
                "difference_percent": diff_pct
            }
        )


def _validate_fastqc_files(fastqc_dir: Path, expected_files: Dict[str, str]) -> ValidationResult:
    """
    Validate FastQC files for a sample.
    
    Args:
        fastqc_dir: Directory containing FastQC files
        expected_files: Dictionary of expected file patterns
        
    Returns:
        ValidationResult
    """
    missing_files = []
    found_files = {}
    
    # Check only for FastQC files
    relevant_keys = [key for key in expected_files if key.startswith("fastqc")]
    
    for key in relevant_keys:
        expected_file = expected_files[key]
        file_path = fastqc_dir / expected_file
        
        if not file_path.exists():
            missing_files.append(expected_file)
        else:
            if key not in found_files:
                found_files[key] = []
            found_files[key].append(str(file_path.name))
    
    if missing_files:
        return ValidationResult(
            status=Status.YELLOW,  # FastQC files are not critical
            message=f"Missing FastQC files: {', '.join(missing_files)}",
            details={"missing": missing_files, "found": found_files}
        )
    
    # Check FastQC report content for ZIP files
    zip_issues = []
    for key in found_files:
        if key.endswith("_zip"):
            for zip_file_name in found_files[key]:
                zip_path = fastqc_dir / zip_file_name
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        summary_file = [f for f in zf.namelist() if f.endswith('/summary.txt')]
                        if not summary_file:
                            zip_issues.append(f"{zip_file_name}:summary.txt")
                        else:
                            # Could analyze summary.txt here for quality issues
                            pass
                except zipfile.BadZipFile:
                    zip_issues.append(f"{zip_file_name} (corrupt)")
    
    if zip_issues:
        return ValidationResult(
            status=Status.YELLOW,
            message=f"Issues with FastQC files: {', '.join(zip_issues)}",
            details={"issues": zip_issues, "found": found_files}
        )
    
    return ValidationResult(
        status=Status.GREEN,
        message="All expected FastQC files found",
        details={"files": found_files}
    )


def _validate_multiqc(multiqc_dir: Path) -> ValidationResult:
    """
    Validate MultiQC report.
    
    Args:
        multiqc_dir: Directory containing MultiQC report
        
    Returns:
        ValidationResult
    """
    # Look for MultiQC report file
    report_patterns = [
        "raw_multiqc_GLbulkRNAseq_report.zip",  # Default GeneLab pattern
        "multiqc_report.html",                  # Standard MultiQC pattern
        "raw_multiqc_report.html"               # Alternative pattern
    ]
    
    for pattern in report_patterns:
        matching_files = list(multiqc_dir.glob(pattern))
        if matching_files:
            return ValidationResult(
                status=Status.GREEN,
                message=f"MultiQC report found: {matching_files[0].name}",
                details={"report": str(matching_files[0])}
            )
    
    return ValidationResult(
        status=Status.YELLOW,  # MultiQC is helpful but not critical
        message="MultiQC report not found",
        details={"expected": ", ".join(report_patterns)}
    )


def _parse_multiqc_metrics(multiqc_path: Path, samples: List[str], paired_end: bool) -> Dict[str, Dict[str, Any]]:
    """
    Parse MultiQC report to extract key QC metrics.
    
    Args:
        multiqc_path: Path to MultiQC zip file
        samples: List of sample IDs
        paired_end: Whether data is paired-end
    
    Returns:
        Dictionary mapping sample IDs to metric dictionaries
    """
    # Initialize metrics dictionary
    metrics = {sample: {} for sample in samples}
    
    try:
        # If it's a zip file, extract the data
        if multiqc_path.name.endswith('.zip'):
            with zipfile.ZipFile(multiqc_path, 'r') as zip_ref:
                # Find the multiqc_data.json file
                data_files = [f for f in zip_ref.namelist() if f.endswith('multiqc_data.json')]
                if not data_files:
                    return metrics
                
                # Read the JSON data
                with zip_ref.open(data_files[0]) as f:
                    data = json.load(TextIOWrapper(f))
                    
                    # Get data from general stats
                    if 'report_general_stats_data' in data and len(data['report_general_stats_data']) > 0:
                        stats = data['report_general_stats_data'][-1]  # Last element has the most data
                        for sample_key, sample_stats in stats.items():
                            # Clean sample name by removing _raw suffix
                            clean_name = sample_key.replace('_raw', '')
                            
                            # For paired-end data, R1/R2 will be separate
                            if '_R1' in clean_name:
                                base_name = clean_name.replace('_R1', '')
                                if base_name in metrics:
                                    metrics[base_name]['r1_total_sequences'] = sample_stats.get('total_sequences', 0)
                                    metrics[base_name]['r1_avg_sequence_length'] = sample_stats.get('avg_sequence_length', 0)
                                    metrics[base_name]['r1_percent_gc'] = sample_stats.get('percent_gc', 0)
                                    metrics[base_name]['r1_percent_duplicates'] = sample_stats.get('percent_duplicates', 0)
                            elif '_R2' in clean_name:
                                base_name = clean_name.replace('_R2', '')
                                if base_name in metrics:
                                    metrics[base_name]['r2_total_sequences'] = sample_stats.get('total_sequences', 0)
                                    metrics[base_name]['r2_avg_sequence_length'] = sample_stats.get('avg_sequence_length', 0)
                                    metrics[base_name]['r2_percent_gc'] = sample_stats.get('percent_gc', 0)
                                    metrics[base_name]['r2_percent_duplicates'] = sample_stats.get('percent_duplicates', 0)
                            else:
                                # Single-end or unsuffixed data
                                if clean_name in metrics:
                                    metrics[clean_name]['total_sequences'] = sample_stats.get('total_sequences', 0)
                                    metrics[clean_name]['avg_sequence_length'] = sample_stats.get('avg_sequence_length', 0)
                                    metrics[clean_name]['percent_gc'] = sample_stats.get('percent_gc', 0)
                                    metrics[clean_name]['percent_duplicates'] = sample_stats.get('percent_duplicates', 0)
                    
                    # Parse quality scores from per-base sequence quality plot
                    if 'report_plot_data' in data and 'fastqc_per_base_sequence_quality_plot' in data['report_plot_data']:
                        qc_data = data['report_plot_data']['fastqc_per_base_sequence_quality_plot']['datasets'][0]['lines']
                        for sample_data in qc_data:
                            name = sample_data['name']
                            clean_name = name.replace('_raw', '')
                            
                            # Calculate mean and median quality scores
                            quality_vals = [pair[1] for pair in sample_data['pairs']]
                            
                            if '_R1' in clean_name:
                                base_name = clean_name.replace('_R1', '')
                                if base_name in metrics:
                                    metrics[base_name]['r1_mean_quality'] = round(sum(quality_vals) / len(quality_vals), 2)
                                    metrics[base_name]['r1_median_quality'] = round(statistics.median(quality_vals), 2)
                            elif '_R2' in clean_name:
                                base_name = clean_name.replace('_R2', '')
                                if base_name in metrics:
                                    metrics[base_name]['r2_mean_quality'] = round(sum(quality_vals) / len(quality_vals), 2)
                                    metrics[base_name]['r2_median_quality'] = round(statistics.median(quality_vals), 2)
                            else:
                                if clean_name in metrics:
                                    metrics[clean_name]['mean_quality'] = round(sum(quality_vals) / len(quality_vals), 2)
                                    metrics[clean_name]['median_quality'] = round(statistics.median(quality_vals), 2)
                    
                    # Parse per-sequence GC content for GC distribution
                    if 'report_plot_data' in data and 'fastqc_per_sequence_gc_content_plot' in data['report_plot_data']:
                        gc_data = data['report_plot_data']['fastqc_per_sequence_gc_content_plot']['datasets'][0]['lines']
                        for sample_data in gc_data:
                            name = sample_data['name']
                            clean_name = name.replace('_raw', '')
                            
                            # Get cumulative GC distribution for percentile metrics
                            gc_values = [pair[0] for pair in sample_data['pairs']]
                            gc_counts = [pair[1] for pair in sample_data['pairs']]
                            gc_cumsum = np.cumsum(gc_counts)
                            gc_cumsum_pct = gc_cumsum / gc_cumsum[-1] * 100 if gc_cumsum[-1] > 0 else gc_cumsum
                            
                            # Find GC at 25th, 50th, and 75th percentiles
                            gc_25 = gc_values[np.searchsorted(gc_cumsum_pct, 25)]
                            gc_50 = gc_values[np.searchsorted(gc_cumsum_pct, 50)]
                            gc_75 = gc_values[np.searchsorted(gc_cumsum_pct, 75)]
                            
                            if '_R1' in clean_name:
                                base_name = clean_name.replace('_R1', '')
                                if base_name in metrics:
                                    metrics[base_name]['r1_gc_25pct'] = gc_25
                                    metrics[base_name]['r1_gc_50pct'] = gc_50
                                    metrics[base_name]['r1_gc_75pct'] = gc_75
                            elif '_R2' in clean_name:
                                base_name = clean_name.replace('_R2', '')
                                if base_name in metrics:
                                    metrics[base_name]['r2_gc_25pct'] = gc_25
                                    metrics[base_name]['r2_gc_50pct'] = gc_50
                                    metrics[base_name]['r2_gc_75pct'] = gc_75
                            else:
                                if clean_name in metrics:
                                    metrics[clean_name]['gc_25pct'] = gc_25
                                    metrics[clean_name]['gc_50pct'] = gc_50
                                    metrics[clean_name]['gc_75pct'] = gc_75
                                    
    except Exception as e:
        print(f"Error parsing MultiQC report: {e}")
    
    return metrics


def _detect_outliers(metrics: Dict[str, Dict[str, Any]], stdev_threshold: float = 2.0) -> Optional[ValidationResult]:
    """
    Detect statistical outliers in QC metrics.
    
    Args:
        metrics: Dictionary of sample metrics
        stdev_threshold: Number of standard deviations for outlier detection
        
    Returns:
        ValidationResult if outliers found, None otherwise
    """
    if not metrics:
        return None
    
    outliers = {}
    
    # Check metrics that should be consistent across samples
    metrics_to_check = [
        'total_sequences', 'avg_sequence_length', 'percent_gc', 
        'r1_total_sequences', 'r2_total_sequences',
        'r1_percent_gc', 'r2_percent_gc',
        'mean_quality', 'median_quality',
        'r1_mean_quality', 'r2_mean_quality',
        'percent_duplicates', 'r1_percent_duplicates', 'r2_percent_duplicates'
    ]
    
    for metric_name in metrics_to_check:
        # Collect values across all samples that have this metric
        values = []
        sample_map = {}
        
        for sample, sample_metrics in metrics.items():
            if metric_name in sample_metrics:
                value = sample_metrics[metric_name]
                if isinstance(value, (int, float)) and not pd.isna(value):
                    values.append(value)
                    sample_map[len(values) - 1] = sample  # Map index to sample name
        
        # If we have enough values for statistics
        if len(values) >= 3:
            try:
                mean_val = statistics.mean(values)
                stdev_val = statistics.stdev(values)
                
                # Identify outliers
                metric_outliers = []
                for i, value in enumerate(values):
                    z_score = (value - mean_val) / stdev_val if stdev_val > 0 else 0
                    if abs(z_score) > stdev_threshold:
                        sample = sample_map[i]
                        metric_outliers.append({
                            "sample": sample,
                            "value": value,
                            "z_score": z_score
                        })
                
                if metric_outliers:
                    outliers[metric_name] = {
                        "mean": mean_val,
                        "stdev": stdev_val,
                        "outliers": metric_outliers
                    }
            except Exception as e:
                print(f"Error calculating statistics for {metric_name}: {e}")
    
    if outliers:
        return ValidationResult(
            status=Status.YELLOW,  # Outliers are warnings, not failures
            message=f"Detected {sum(len(v['outliers']) for v in outliers.values())} outliers across {len(outliers)} metrics",
            details={"outliers": outliers}
        )
    
    return None 