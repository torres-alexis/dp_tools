"""
Logging utilities for validation and verification.
"""
import enum
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Union


class Status(enum.Enum):
    """Status codes for validation results"""
    GREEN = "GREEN"  # Passed validation
    YELLOW = "YELLOW"  # Warning, but acceptable
    RED = "RED"  # Failed validation, but non-critical
    HALT = "HALT"  # Critical failure that should halt processing


class ValidationResult:
    """
    Represents a validation check result with status, message, and details.
    """
    def __init__(self, status: Status, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize a validation result.
        
        Args:
            status: Status of the validation (GREEN, YELLOW, RED, HALT)
            message: Human-readable message describing the result
            details: Optional dictionary with additional details
        """
        self.status = status
        self.message = message
        self.details = details or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "status": self.status.value,
            "message": self.message,
            "details": self.details
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ValidationResult':
        """Create instance from dictionary"""
        return cls(
            status=Status[data["status"]],
            message=data["message"],
            details=data.get("details", {})
        )


class ValidationResultWithOutliers(ValidationResult):
    """
    Validation result that includes outliers information.
    """
    def __init__(self, status: Status, message: str, details: Optional[Dict[str, Any]] = None, 
                 outliers: Optional[List[str]] = None):
        """
        Initialize a validation result with outliers.
        
        Args:
            status: Status of the validation (GREEN, YELLOW, RED, HALT)
            message: Human-readable message describing the result
            details: Optional dictionary with additional details
            outliers: List of outlier sample IDs
        """
        super().__init__(status, message, details)
        self.outliers = outliers or []
        if outliers:
            if "outliers" not in self.details:
                self.details["outliers"] = outliers
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        result = super().to_dict()
        result["outliers"] = self.outliers
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ValidationResultWithOutliers':
        """Create instance from dictionary"""
        return cls(
            status=Status[data["status"]],
            message=data["message"],
            details=data.get("details", {}),
            outliers=data.get("outliers", [])
        )


class ValidationLogger:
    """
    Logger for validation operations that tracks results and can produce reports.
    """
    def __init__(self, log_file: Optional[Union[str, Path]] = None):
        """
        Initialize a validation logger.
        
        Args:
            log_file: Optional path to log file. If provided, results will be written 
                     to this file as they are logged.
        """
        self.log_file = log_file
        self.results = []
        self.stats = {}  # Track quantitative stats
        
        # Set up logging
        self.logger = logging.getLogger("validation")
        
        # Create/overwrite log file with header if provided
        if log_file:
            with open(log_file, "w") as f:
                f.write("component,sample_id,check_name,status,message,details\n")
    
    def log(self, component: str, sample_id: str, check_name: str, 
           result: ValidationResult) -> None:
        """
        Log a validation result.
        
        Args:
            component: Component being validated
            sample_id: Sample ID being validated
            check_name: Name of the check being performed
            result: ValidationResult object with validation result
        """
        # Extract just the sample name from CSV row
        sample_name = sample_id.split(',')[-1].strip() if ',' in sample_id else sample_id
        
        entry = {
            "component": component,
            "sample_id": sample_name,
            "check_name": check_name, 
            "status": result.status.value,
            "message": result.message,
            "details": str(result.details) if result.details else ""
        }
        self.results.append(entry)
        
        # Track stats if provided in details
        stats = result.details.get("stats", None)
        if stats:
            if component not in self.stats:
                self.stats[component] = {}
            if sample_name not in self.stats[component]:
                self.stats[component][sample_name] = {}
            self.stats[component][sample_name][check_name] = stats
        
        # Log to internal logger
        if stats:
            # Quantitative check
            self.logger.info(f"{component} - {sample_name} - {check_name}:")
            for stat_name, value in stats.items():
                self.logger.info(f"  {stat_name}: {value}")
        else:
            # Qualitative check
            self.logger.info(f"{component} - {sample_name} - {check_name}: {result.status.value}")
            if result.details:
                self.logger.info(f"  Details: {result.details}")
        
        # Write to log file if provided
        if self.log_file:
            self._write_to_log_file(entry)
    
    def _write_to_log_file(self, entry: Dict[str, str]) -> None:
        """Write an entry to the log file"""
        if not self.log_file:
            return
        
        # Escape any commas in fields
        safe_fields = [
            str(field).replace('"', '""') if ',' in str(field) else str(field)
            for field in [
                entry["component"], 
                entry["sample_id"], 
                entry["check_name"], 
                entry["status"], 
                entry["message"], 
                entry["details"]
            ]
        ]
        
        # Quote fields that need it
        csv_fields = [
            f'"{field}"' if ',' in field else field 
            for field in safe_fields
        ]
        
        # Append to log file
        with open(self.log_file, "a") as f:
            f.write(f"{','.join(csv_fields)}\n")
    
    def get_status(self) -> Status:
        """
        Get overall validation status.
        
        Returns:
            Status: Highest severity status found
        """
        if any(r["status"] == Status.HALT.value for r in self.results):
            return Status.HALT
        if any(r["status"] == Status.RED.value for r in self.results):
            return Status.RED
        if any(r["status"] == Status.YELLOW.value for r in self.results):
            return Status.YELLOW
        return Status.GREEN
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of validation results.
        
        Returns:
            Dict: Summary statistics
        """
        summary = {
            "total_checks": len(self.results),
            "status_counts": {
                "GREEN": 0,
                "YELLOW": 0,
                "RED": 0,
                "HALT": 0
            },
            "components": {},
            "samples": {}
        }
        
        # Count statuses
        for result in self.results:
            status = result["status"]
            summary["status_counts"][status] = summary["status_counts"].get(status, 0) + 1
            
            # Track by component
            component = result["component"]
            if component not in summary["components"]:
                summary["components"][component] = {
                    "total": 0,
                    "GREEN": 0,
                    "YELLOW": 0,
                    "RED": 0,
                    "HALT": 0
                }
            summary["components"][component]["total"] += 1
            summary["components"][component][status] += 1
            
            # Track by sample
            sample = result["sample_id"]
            if sample not in summary["samples"]:
                summary["samples"][sample] = {
                    "total": 0,
                    "GREEN": 0,
                    "YELLOW": 0,
                    "RED": 0,
                    "HALT": 0
                }
            summary["samples"][sample]["total"] += 1
            summary["samples"][sample][status] += 1
        
        # Calculate overall status
        summary["overall_status"] = self.get_status().value
        
        return summary 