"""
Basic file handling utilities for commonly used operations in data processing.
"""
import os
import re
import gzip
import shutil
import zipfile
import subprocess
import pandas as pd
from pathlib import Path
from io import TextIOWrapper
from typing import List, Dict, Optional, Union, Set, Any


def check_file_exists(file_path: Union[str, Path]) -> bool:
    """
    Check if a file exists.
    
    Args:
        file_path: Path to the file
        
    Returns:
        bool: True if file exists, False otherwise
    """
    return Path(file_path).exists()


def check_gzip_integrity(file_path: Union[str, Path]) -> bool:
    """
    Check if a gzipped file is valid.
    
    Args:
        file_path: Path to the gzipped file
        
    Returns:
        bool: True if valid gzip file, False otherwise
    """
    try:
        with gzip.open(file_path, 'rb') as f:
            # Read a small chunk to test integrity
            f.read(1024)
        return True
    except Exception:
        return False


def load_samples(samples_source: Union[str, Path, List[str]], sample_column: str = "Sample Name") -> List[str]:
    """
    Load sample IDs from various sources.
    
    Args:
        samples_source: Can be one of:
            - Path to samples.txt file (one sample ID per line)
            - Path to runsheet CSV/TSV file
            - List of sample IDs
        sample_column: Column name to use if samples_source is a runsheet
        
    Returns:
        List[str]: List of sample IDs
    """
    # If it's already a list, just return it
    if isinstance(samples_source, list):
        return samples_source
    
    # Convert to Path object
    path = Path(samples_source)
    
    # Check if it exists
    if not path.exists():
        raise FileNotFoundError(f"Sample source not found: {samples_source}")
    
    # Check file extension to determine how to load
    if path.suffix.lower() in ['.csv', '.tsv']:
        # It's a runsheet
        try:
            if path.suffix.lower() == '.csv':
                df = pd.read_csv(path)
            else:
                df = pd.read_csv(path, sep='\t')
            
            if sample_column not in df.columns:
                raise ValueError(f"Column '{sample_column}' not found in runsheet. Available columns: {', '.join(df.columns)}")
            
            # Extract samples from the column
            samples = df[sample_column].unique().tolist()
            return samples
        except Exception as e:
            raise ValueError(f"Failed to load samples from runsheet: {e}")
    else:
        # Assume it's a simple text file with one sample per line
        samples = []
        with open(path, 'r') as f:
            for line in f:
                sample_id = line.strip()
                if sample_id and not sample_id.startswith('#'):
                    samples.append(sample_id)
        return samples


def get_fastq_pattern(sample_id: str, paired_end: bool = True, read_num: Optional[int] = None) -> str:
    """
    Get the pattern for FASTQ files based on sample ID and read type.
    
    Args:
        sample_id: Sample ID
        paired_end: Whether the data is paired-end
        read_num: Read number (1 or 2 for paired-end)
        
    Returns:
        str: FASTQ file pattern
    """
    if not paired_end:
        return f"{sample_id}*.fastq.gz"
    
    if read_num is None:
        return f"{sample_id}*R[1-2]*.fastq.gz"
    elif read_num == 1:
        return f"{sample_id}*R1*.fastq.gz"
    elif read_num == 2:
        return f"{sample_id}*R2*.fastq.gz"
    else:
        raise ValueError(f"Invalid read_num: {read_num}. Must be 1 or 2.")


def count_reads_in_fastq(fastq_path: Union[str, Path]) -> int:
    """
    Count the number of reads in a FASTQ file using shell commands for efficiency.
    
    Args:
        fastq_path: Path to the FASTQ file (gzipped or not)
        
    Returns:
        int: Number of reads
    """
    try:
        if str(fastq_path).endswith('.gz'):
            cmd = f"gunzip -c {fastq_path} | wc -l"
        else:
            cmd = f"wc -l {fastq_path}"
        
        output = subprocess.check_output(cmd, shell=True).decode().strip()
        line_count = int(output.split()[0])
        
        # Each FASTQ record has 4 lines
        return line_count // 4
    except Exception as e:
        raise RuntimeError(f"Failed to count reads in {fastq_path}: {e}")


def find_files(directory: Union[str, Path], pattern: str) -> List[Path]:
    """
    Find files matching a pattern in a directory.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern to match
        
    Returns:
        List[Path]: List of paths to matching files
    """
    return list(Path(directory).glob(pattern))


def check_fastq_pairs(r1_path: Union[str, Path], r2_path: Union[str, Path]) -> Dict:
    """
    Check that paired FASTQ files have the same number of reads.
    
    Args:
        r1_path: Path to R1 FASTQ file
        r2_path: Path to R2 FASTQ file
        
    Returns:
        Dict: Dictionary with read counts and match status
    """
    r1_count = count_reads_in_fastq(r1_path)
    r2_count = count_reads_in_fastq(r2_path)
    
    return {
        "r1_count": r1_count,
        "r2_count": r2_count,
        "matching": r1_count == r2_count
    }


def ensure_directory_exists(directory: Union[str, Path]) -> Path:
    """
    Create directory if it doesn't exist.
    
    Args:
        directory: Directory path to create
        
    Returns:
        Path: Path object for the directory
    """
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def create_symlink(source: Union[str, Path], target: Union[str, Path], force: bool = True) -> bool:
    """
    Create a symbolic link with proper error handling.
    
    Args:
        source: Source path
        target: Target path for the symlink
        force: Whether to overwrite existing targets
        
    Returns:
        bool: True if successful, False otherwise
    """
    source_path = Path(source)
    target_path = Path(target)
    
    # Check if source exists
    if not source_path.exists():
        print(f"Source path does not exist: {source_path}")
        return False
    
    try:
        # Get the real path to handle symlinked sources correctly
        real_source = os.path.realpath(source_path)
        
        # Remove destination if it already exists and force is True
        if force and (target_path.exists() or target_path.is_symlink()):
            if target_path.is_dir() and not target_path.is_symlink():
                shutil.rmtree(target_path)
            else:
                os.unlink(target_path)
        
        # Create parent directory if it doesn't exist
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create symlink
        os.symlink(real_source, target_path)
        return True
    except Exception as e:
        print(f"Failed to create symlink from {source_path} to {target_path}: {e}")
        return False


def stage_to_location(source_path: Union[str, Path], target_dir: Union[str, Path]) -> Dict[str, int]:
    """
    Stage files from source path to target directory using symbolic links.
    
    If source_path is a directory, all its contents will be linked in target_dir.
    If source_path is a file, it will be linked in target_dir.
    
    Args:
        source_path: Source path (file or directory)
        target_dir: Target directory
        
    Returns:
        Dict: Statistics about staged files
    """
    source_path = Path(source_path)
    target_dir = Path(target_dir)
    
    # Ensure target directory exists
    ensure_directory_exists(target_dir)
    
    stats = {"success": 0, "error": 0}
    
    if source_path.exists():
        if source_path.is_dir():
            # For directories, link their contents directly into target_dir
            for item in os.listdir(source_path):
                src = os.path.join(source_path, item)
                dst = os.path.join(target_dir, item)
                
                if create_symlink(src, dst):
                    stats["success"] += 1
                else:
                    stats["error"] += 1
        else:
            # For single files
            dst = target_dir / source_path.name
            
            if create_symlink(source_path, dst):
                stats["success"] += 1
            else:
                stats["error"] += 1
    else:
        print(f"Source path does not exist: {source_path}")
        stats["error"] += 1
    
    return stats


def check_fastqgz_file_contents(file: Path, count_lines_to_check: int = 400000) -> Dict[str, Any]:
    """
    Check FASTQ.GZ file integrity by examining headers and decompressing as a stream.
    
    Args:
        file: Input FASTQ.GZ file path
        count_lines_to_check: Maximum number of lines to check (negative for no limit)
        
    Returns:
        Dict: Results of the check with status and details
    """
    lines_with_issues: List[int] = []
    
    try:
        with gzip.open(file, "rb") as f:
            for i, byte_line in enumerate(f):
                # Check if lines counted equals the limit input
                if i + 1 == count_lines_to_check:
                    break
                
                line = byte_line.decode()
                # Every fourth line should be an identifier
                expected_identifier_line = i % 4 == 0
                # Check if line is actually an identifier line
                if expected_identifier_line and line[0] != "@":
                    lines_with_issues.append(i + 1)
        
        if lines_with_issues:
            return {
                "valid": False,
                "message": f"FASTQ.GZ file has issues in {len(lines_with_issues)} lines",
                "lines_with_issues": lines_with_issues
            }
        else:
            return {
                "valid": True,
                "message": f"Checked {min(i+1, count_lines_to_check)} lines with no issues",
                "lines_checked": min(i+1, count_lines_to_check)
            }
    except (EOFError, gzip.BadGzipFile) as e:
        return {
            "valid": False,
            "message": f"Error during decompression: {str(e)}",
            "error": str(e)
        }


def check_bam_file_integrity(bam_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Check BAM file integrity using samtools quickcheck.
    
    Args:
        bam_path: Path to the BAM file
        
    Returns:
        Dict: Results of the check with status and details
    """
    try:
        # Run samtools quickcheck
        result = subprocess.run(
            ["samtools", "quickcheck", "-v", str(bam_path)],
            capture_output=True,
            text=True
        )
        
        # If return code is 0, the file is valid
        if result.returncode == 0:
            return {
                "valid": True,
                "message": "BAM file is valid"
            }
        else:
            return {
                "valid": False,
                "message": "BAM file is invalid",
                "details": result.stderr.strip()
            }
    except Exception as e:
        return {
            "valid": False,
            "message": f"Error checking BAM file: {str(e)}",
            "error": str(e)
        }


def parse_trimming_report(report_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Parse a TrimGalore trimming report to extract key metrics.
    
    Args:
        report_path: Path to the TrimGalore trimming report file
        
    Returns:
        Dict: Metrics extracted from the trimming report
    """
    report_path = Path(report_path)
    
    with open(report_path, 'r') as f:
        content = f.read()
    
    # Initialize stats with default values
    stats = {
        "adapters_found": False,
        "adapter_type": "unknown",
        "adapter_sequence": "",
        "adapter_trimmed_percentage": 0.0,
        "quality_trimmed_percentage": 0.0,
        "total_processed": 0,
        "total_written": 0,
        "total_bp_processed": 0,
        "bp_quality_trimmed": 0,
    }
    
    # Try to parse total reads
    total_match = re.search(r"Total reads processed:\s*([0-9,]+)", content)
    if total_match:
        stats["total_processed"] = int(total_match.group(1).replace(',', ''))
    
    # Try to parse written reads
    written_match = re.search(r"Reads written \(passing filters\):\s*([0-9,]+)\s+\(([0-9.]+)%\)", content)
    if written_match:
        stats["total_written"] = int(written_match.group(1).replace(',', ''))
        stats["written_percentage"] = float(written_match.group(2))
    
    # Adapter type detection
    adapter_match = re.search(r"Adapter sequence:[^']+'([ACGT]+)'[^(]*\(([^,;]+)", content)
    if adapter_match:
        stats["adapters_found"] = True
        stats["adapter_sequence"] = adapter_match.group(1)
        stats["adapter_type"] = adapter_match.group(2).strip()
    
    # Fallback to simpler pattern without trying to guess the type
    if not stats["adapters_found"]:
        simple_match = re.search(r"Adapter sequence: '([ACGT]+)'", content)
        if simple_match:
            stats["adapters_found"] = True
            stats["adapter_sequence"] = simple_match.group(1)
            stats["adapter_type"] = "Unspecified"
    
    # Try to find adapter trimming stats
    adapter_match = re.search(r"Sequences removed because of trimming:\s*([0-9,]+)\s+\(([0-9.]+)%\)", content)
    if adapter_match:
        stats["adapters_trimmed"] = int(adapter_match.group(1).replace(',', ''))
        stats["adapter_trimmed_percentage"] = float(adapter_match.group(2))
        if stats["adapter_trimmed_percentage"] > 0:
            stats["adapters_found"] = True
    
    # Also try the format: "Reads with adapters"
    adapter_match = re.search(r"Reads with adapters:\s*([0-9,]+)\s+\(([0-9.]+)%\)", content)
    if adapter_match:
        stats["adapters_trimmed"] = int(adapter_match.group(1).replace(',', ''))
        stats["adapter_trimmed_percentage"] = float(adapter_match.group(2))
        if stats["adapter_trimmed_percentage"] > 0:
            stats["adapters_found"] = True
    
    # Try to find quality trimming stats
    quality_match = re.search(r"Quality-trimmed:\s*([0-9,]+)\s+bp\s+\(([0-9.]+)%\)", content)
    if quality_match:
        stats["bp_quality_trimmed"] = int(quality_match.group(1).replace(',', ''))
        stats["quality_trimmed_percentage"] = float(quality_match.group(2))
    
    # Alternative quality trimming pattern
    if "quality_trimmed_percentage" not in stats or stats["quality_trimmed_percentage"] == 0:
        quality_match = re.search(r"Quality value for trimming\s+([0-9]+)", content)
        if quality_match:
            # If we have a quality value but no explicit percentage, estimate from total trimmed
            total_bp_match = re.search(r"Total basepairs processed:\s*([0-9,]+)", content)
            written_bp_match = re.search(r"Total written \(filtered\):\s*([0-9,]+)", content)
            
            if total_bp_match and written_bp_match:
                total_bp = int(total_bp_match.group(1).replace(',', ''))
                written_bp = int(written_bp_match.group(1).replace(',', ''))
                if total_bp > 0:
                    stats["total_bp_processed"] = total_bp
                    stats["quality_trimmed_percentage"] = 100 * (total_bp - written_bp) / total_bp
    
    return stats


def extract_adapter_content_from_fastqc(fastqc_zip_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Extract adapter content information from a FastQC zip file.
    
    Args:
        fastqc_zip_path: Path to FastQC zip file
        
    Returns:
        Dict: Adapter content information
    """
    fastqc_zip_path = Path(fastqc_zip_path)
    
    adapter_stats = {
        "adapter_found": False,
        "adapter_percentage": 0.0,
        "adapter_details": {}
    }
    
    try:
        with zipfile.ZipFile(fastqc_zip_path, 'r') as zip_ref:
            # Find the fastqc_data.txt file
            data_file = None
            for file in zip_ref.namelist():
                if file.endswith('fastqc_data.txt'):
                    data_file = file
                    break
            
            if not data_file:
                return adapter_stats
            
            # Parse the file
            with zip_ref.open(data_file) as f:
                text_f = TextIOWrapper(f)
                in_adapter_section = False
                for line in text_f:
                    line = line.strip()
                    
                    # Find adapter content section
                    if line == ">>Adapter Content":
                        in_adapter_section = True
                        continue
                    
                    if in_adapter_section:
                        if line.startswith('>>END_MODULE'):
                            break
                        
                        if line.startswith('#'):
                            continue  # Skip header line
                        
                        # Parse adapter content data
                        parts = line.split('\t')
                        if len(parts) > 2:
                            position = parts[0]
                            # Get maximum adapter percentage from any adapter type
                            max_pct = max([float(x.strip()) for x in parts[1:]])
                            
                            adapter_stats["adapter_details"][position] = max_pct
                            
                            # If any position has >0.1% adapter, consider it found
                            if max_pct > 0.1:
                                adapter_stats["adapter_found"] = True
                                adapter_stats["adapter_percentage"] = max(adapter_stats["adapter_percentage"], max_pct)
    except Exception as e:
        adapter_stats["error"] = str(e)
    
    return adapter_stats


def check_samples_in_multiqc(multiqc_zip_path: Union[str, Path], samples: List[str]) -> Dict[str, Any]:
    """
    Check if all expected samples are present in a MultiQC report.
    
    Args:
        multiqc_zip_path: Path to the MultiQC zip file
        samples: List of expected sample names/IDs to check for
        
    Returns:
        Dict: Results of the check
    """
    multiqc_zip_path = Path(multiqc_zip_path)
    
    results = {
        "all_samples_found": False,
        "samples_found": [],
        "samples_missing": [],
        "extra_samples": []
    }
    
    try:
        with zipfile.ZipFile(multiqc_zip_path, 'r') as zip_ref:
            # Look for multiqc_general_stats.txt file
            stats_files = [f for f in zip_ref.namelist() if f.endswith('multiqc_general_stats.txt')]
            
            if not stats_files:
                results["error"] = "Could not find multiqc_general_stats.txt in the MultiQC zip file"
                return results
            
            # Use the first stats file found
            stats_file = stats_files[0]
            
            with zip_ref.open(stats_file) as f:
                content = TextIOWrapper(f).read()
                
                # Parse sample names from the file
                # This is a tab-delimited file with the first column being sample names
                found_samples = []
                for line in content.split('\n'):
                    if line.strip() and not line.startswith('#'):
                        parts = line.split('\t')
                        if parts:
                            sample_name = parts[0].strip()
                            found_samples.append(sample_name)
                
                # Remove header if present
                if found_samples and found_samples[0].lower() == 'sample':
                    found_samples = found_samples[1:]
                
                # Normalize sample names by removing extensions and common suffixes
                normalized_found = [s.split('.')[0].split('_R')[0] for s in found_samples]
                normalized_expected = [s.split('.')[0].split('_R')[0] for s in samples]
                
                # Check which samples were found and which are missing
                results["samples_found"] = [s for s in samples if any(s.split('.')[0].split('_R')[0] in found for found in normalized_found)]
                results["samples_missing"] = [s for s in samples if s not in results["samples_found"]]
                results["extra_samples"] = [s for s in found_samples if not any(expected.split('.')[0].split('_R')[0] in s for expected in normalized_expected)]
                
                results["all_samples_found"] = len(results["samples_missing"]) == 0
    except Exception as e:
        results["error"] = str(e)
    
    return results


def get_paired_end_from_runsheet(runsheet_path: Union[str, Path]) -> bool:
    """
    Get paired-end status from runsheet.
    
    For RNA-seq data, simply checks the 'paired_end' column in the runsheet.
    
    Args:
        runsheet_path: Path to runsheet CSV file
        
    Returns:
        bool: True if paired-end, False if single-end
        
    Raises:
        ValueError: If paired-end status cannot be determined from the runsheet
    """
    try:
        runsheet = Path(runsheet_path)
        if runsheet.suffix.lower() == '.csv':
            df = pd.read_csv(runsheet)
        elif runsheet.suffix.lower() == '.tsv':
            df = pd.read_csv(runsheet, sep='\t')
        else:
            df = pd.read_csv(runsheet)  # Try default CSV parsing
        
        # Check specifically for 'paired_end' column
        if 'paired_end' not in df.columns:
            raise ValueError(
                f"Required 'paired_end' column not found in runsheet. "
                f"Available columns: {', '.join(df.columns)}"
            )
        
        # Get the first value
        value = df['paired_end'].iloc[0]
        
        # Use equality check not identity check
        if value == True:
            return True
        elif value == False:
            return False
        else:
            raise ValueError(f"paired_end value must be exactly True or False, got: {value} (type: {type(value).__name__})")
            
    except Exception as e:
        if isinstance(e, ValueError) and "Required 'paired_end' column not found" in str(e):
            raise e
        raise ValueError(f"Failed to extract paired-end status from runsheet: {str(e)}") 