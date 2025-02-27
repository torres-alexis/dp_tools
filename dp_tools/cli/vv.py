"""
Command-line interface for running validation and verification on pipeline outputs.

This module provides commands for validating and staging files for various pipeline components,
with a focus on ease of use and clear reporting.
"""
import os
import json
import logging
from pathlib import Path
from typing import Optional, List, Union

import click

from dp_tools.utils.logging import Status, ValidationResult
from dp_tools.utils.mover import (
    move_files
)
from dp_tools.assays.rnaseq.raw_reads import validate_raw_reads
from dp_tools.utils.file_utils import load_samples


@click.group()
def vv():
    """Data processing pipeline validation and verification tools."""
    pass


@vv.group()
def rnaseq():
    """RNA-seq pipeline validation commands."""
    pass


@rnaseq.command(name="raw-reads")
@click.option("--fastq-dir", required=True, type=click.Path(exists=True), 
              help="Directory containing raw FASTQ files")
@click.option("--fastqc-dir", type=click.Path(exists=True), 
              help="Directory containing FastQC reports (optional)")
@click.option("--multiqc-dir", type=click.Path(exists=True), 
              help="Directory containing MultiQC report (optional)")
@click.option("--outdir", required=True, type=click.Path(), 
              help="Output directory for staged files")
@click.option("--samples", type=str, 
              help="Comma-separated list of sample IDs, path to samples.txt file, or path to runsheet")
@click.option("--sample-column", type=str, default="Sample Name", 
              help="Column name in runsheet containing sample IDs (default: 'Sample Name')")
@click.option("--paired-end/--single-end", default=True, 
              help="Whether data is paired-end (default: paired-end)")
@click.option("--assay-suffix", default="_GLbulkRNAseq", 
              help="Suffix for assay-specific files (default: _GLbulkRNAseq)")
@click.option("--log-file", type=click.Path(), 
              help="Path to log file (optional)")
@click.option("--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]), 
              default="INFO", help="Logging level (default: INFO)")
@click.option("--json-out", type=click.Path(), 
              help="Path to JSON output file (optional)")
@click.option("--dry-run", is_flag=True, 
              help="Show what would be done without copying files")
@click.option("--validate-only", is_flag=True, 
              help="Validate files without staging")
@click.option("--stage-only", is_flag=True, 
              help="Stage files without validation")
def validate_raw_reads_cmd(
    fastq_dir: str,
    outdir: str,
    fastqc_dir: Optional[str] = None,
    multiqc_dir: Optional[str] = None,
    samples: Optional[str] = None,
    sample_column: str = "Sample Name",
    paired_end: bool = True,
    assay_suffix: str = "_GLbulkRNAseq",
    log_file: Optional[str] = None,
    log_level: str = "INFO",
    json_out: Optional[str] = None,
    dry_run: bool = False,
    validate_only: bool = False,
    stage_only: bool = False
):
    """Validate and stage raw RNA-seq FASTQ files.
    
    This command performs two main functions:
    1. Validates raw FASTQ files for quality and completeness
    2. Stages the files into the expected output directory structure
    
    You can choose to run only validation or only staging with the appropriate flags.
    """
    # Configure logging
    configure_staging_logger(log_file, getattr(logging, log_level))
    
    # Process sample input
    sample_list = []
    if samples:
        if "," in samples and not os.path.exists(samples):
            # Treat as comma-separated list
            sample_list = [s.strip() for s in samples.split(",")]
            logging.info(f"Using {len(sample_list)} samples from command line")
        else:
            # Treat as file path
            sample_list = load_samples(samples, sample_column)
            logging.info(f"Loaded {len(sample_list)} samples from {samples}")
    else:
        # Try to infer samples from FASTQ directory
        sample_dict = infer_sample_files(fastq_dir)
        sample_list = list(sample_dict.keys())
        if sample_list:
            logging.info(f"Inferred {len(sample_list)} samples from FASTQ files: {', '.join(sample_list)}")
        else:
            logging.error("No samples specified and could not infer samples from FASTQ files")
            return
    
    results = {}
    
    # Validate raw reads if not stage_only
    if not stage_only:
        logging.info("Starting raw reads validation...")
        validation_results = validate_raw_reads(
            fastq_dir=fastq_dir,
            samples=sample_list,
            paired_end=paired_end
        )
        
        # Process validation results
        status_counts = {
            Status.GREEN.name: 0,
            Status.YELLOW.name: 0,
            Status.RED.name: 0,
            Status.HALT.name: 0
        }
        
        for sample, result in validation_results.items():
            status_counts[result.status.name] += 1
        
        overall_status = Status.GREEN
        if status_counts[Status.HALT.name] > 0:
            overall_status = Status.HALT
        elif status_counts[Status.RED.name] > 0:
            overall_status = Status.RED
        elif status_counts[Status.YELLOW.name] > 0:
            overall_status = Status.YELLOW
        
        results["validation"] = {
            "overall_status": overall_status.name,
            "sample_count": len(validation_results),
            "status_counts": status_counts,
            "results": {sample: result.to_dict() for sample, result in validation_results.items()}
        }
        
        # Print summary
        click.echo(f"\nValidation Summary ({overall_status.name}):")
        click.echo(f"Total samples: {len(validation_results)}")
        click.echo(f"Passed: {status_counts[Status.GREEN.name]}")
        click.echo(f"Warnings: {status_counts[Status.YELLOW.name]}")
        click.echo(f"Failed: {status_counts[Status.RED.name]}")
        click.echo(f"Critical errors: {status_counts[Status.HALT.name]}")
        
        # Exit early if validation fails and requested not to stage
        if (overall_status == Status.HALT or overall_status == Status.RED) and not stage_only:
            click.echo("\nValidation failed. Use --stage-only to force staging.")
            if json_out:
                with open(json_out, 'w') as f:
                    json.dump(results, f, indent=2)
            return
    
    # Stage files if not validate_only
    if not validate_only:
        logging.info("Starting file staging...")
        
        # Create output directory if it doesn't exist and not in dry run mode
        outdir_path = Path(outdir)
        if not outdir_path.exists() and not dry_run:
            os.makedirs(outdir_path)
        
        # Stage raw reads files
        staging_results = stage_raw_reads(
            base_dir=outdir,
            raw_fastq_dir=fastq_dir,
            raw_fastqc_dir=fastqc_dir,
            raw_multiqc_dir=multiqc_dir,
            assay_suffix=assay_suffix,
            dry_run=dry_run
        )
        
        # Validate staging results
        staging_validation = validate_staging(
            staging_results,
            required_file_types=["fastq"],
            min_files={"fastq": len(sample_list) * (2 if paired_end else 1)}
        )
        
        results["staging"] = {
            "success": staging_validation["success"],
            "staged_files": staging_results,
            "validation": staging_validation
        }
        
        # Print summary
        click.echo(f"\nStaging Summary ({'Success' if staging_validation['success'] else 'Failed'}):")
        for file_type, staged_info in staging_results.items():
            file_count = sum(len(files) for target_dir, files in staged_info.items())
            click.echo(f"  {file_type}: {file_count} files")
        
        if not staging_validation["success"]:
            click.echo("\nStaging validation issues:")
            if staging_validation["missing_types"]:
                click.echo(f"  Missing file types: {', '.join(staging_validation['missing_types'])}")
            
            for file_type, info in staging_validation["insufficient_files"].items():
                click.echo(f"  Insufficient {file_type} files: {info['actual']}/{info['expected']}")
    
    # Write JSON output if requested
    if json_out:
        with open(json_out, 'w') as f:
            json.dump(results, f, indent=2)
        logging.info(f"Results written to {json_out}")


if __name__ == "__main__":
    vv() 