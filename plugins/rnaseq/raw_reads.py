"""
RNA-seq raw reads structure definition.
"""
from pathlib import Path
from typing import Dict, List, Optional, Union

# Output structure config
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