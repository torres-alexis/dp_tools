"""
File organization utility that uses component-specific layout definitions.

This module detects the assay type and component, then uses the appropriate 
layout definitions from each component to organize files into their correct locations.
"""
import os
import sys
import shutil
import importlib
import glob
from pathlib import Path
from typing import Dict, List, Union, Optional, Any, Callable


class Mover:
    """
    File organization utility that uses component-specific layout definitions.
    """
    
    def __init__(self, assay_type: str, plugin_dir: Optional[str] = None):
        """
        Initialize mover for a specific assay type.
        
        Args:
            assay_type: The type of assay (e.g., 'rnaseq', 'atacseq')
            plugin_dir: Directory containing alternate assay implementation
        """
        self.assay_type = assay_type
        self.component_modules = {}
        self.plugin_dir = plugin_dir
    
    def _load_component_module(self, component: str):
        """
        Load a component module dynamically.
        
        Will first try to load from the built-in assays, then from plugin directory if specified.
        
        Args:
            component: The component name (e.g., 'raw_reads', 'trimmed_reads')
            
        Returns:
            The loaded module or None if not found
        """
        # First try built-in modules unless we have a plugin
        if not self.plugin_dir:
            try:
                module_path = f"dp_tools.assays.{self.assay_type}.{component}"
                component_module = importlib.import_module(module_path)
                return component_module
            except ImportError as e:
                raise ValueError(f"Could not load component '{component}' for assay type '{self.assay_type}': {str(e)}")
        
        # Using a plugin directory
        plugin_path = Path(self.plugin_dir) / f"{component}.py"
        if not plugin_path.exists():
            raise ValueError(f"Could not find component '{component}' in plugin directory: {self.plugin_dir}")
            
        try:
            # Dynamically load the module from the plugin path
            spec = importlib.util.spec_from_file_location(
                f"dp_tools_plugin.{component}", 
                plugin_path
            )
            component_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(component_module)
            return component_module
        except Exception as e:
            raise ValueError(f"Failed to load plugin {plugin_path}: {e}")
    
    def move_files(self, 
                  component: str, 
                  files: List[Union[str, Path]], 
                  output_dir: Union[str, Path],
                  dry_run: bool = False,
                  use_symlinks: bool = False) -> Dict[str, List[Path]]:
        """
        Organize files to their proper locations based on the component-specific layout.
        
        Args:
            component: The component of the assay (e.g., 'raw_reads', 'trimmed_reads')
            files: List of files to organize
            output_dir: Base output directory
            dry_run: If True, only print what would be done
            use_symlinks: If True, create symbolic links instead of copying files
            
        Returns:
            Dictionary of organized files by target directory
        """
        # Load the component module if not already loaded
        if component not in self.component_modules:
            self.component_modules[component] = self._load_component_module(component)
        
        component_module = self.component_modules[component]
        
        # Get the layout from the component module's STRUCTURE attribute
        if not hasattr(component_module, "STRUCTURE"):
            raise ValueError(f"Component '{component}' does not define a STRUCTURE")
        
        structure = getattr(component_module, "STRUCTURE")
        if not structure or not isinstance(structure, dict):
            raise ValueError(f"Invalid STRUCTURE in component '{component}'")
        
        # For the RNA-seq assay, extract the relevant component structure
        if self.assay_type == "rnaseq":
            # Navigate through the nested structure if needed
            # STRUCTURE["rnaseq"]["microbes"]["components"][component]["outputs"]
            try:
                component_structure = structure["rnaseq"]["microbes"]["components"][component]["outputs"]
            except KeyError:
                try:
                    # Try simpler structure STRUCTURE[component]["outputs"]
                    component_structure = structure[component]["outputs"]
                except KeyError:
                    # Simplest structure - direct mapping
                    component_structure = structure
        else:
            # For other assays, use the structure directly if it's simple
            if isinstance(structure, dict) and all(isinstance(v, str) for v in structure.values()):
                component_structure = structure
            else:
                # Try to extract from a more complex structure
                try:
                    component_structure = structure[self.assay_type][component]["outputs"]
                except KeyError:
                    raise ValueError(f"Could not find layout for component '{component}' in structure")
        
        output_dir = Path(output_dir)
        
        # Create result dictionary
        result = {str(output_dir / subdir): [] for subdir in component_structure.values()}
        
        # Process each file
        for file_path in files:
            file_path = Path(file_path)
            
            if not file_path.exists():
                print(f"Warning: File not found: {file_path}")
                continue
            
            # Find matching file type in layout
            target_subdir = None
            for file_type, subdir in component_structure.items():
                # Each file gets sorted by file type
                if self._is_file_type(file_path.name, file_type, component):
                    target_subdir = subdir
                    break
            
            if not target_subdir:
                print(f"Warning: No matching file type found for file: {file_path}")
                continue
            
            # Create target directory
            full_target_dir = output_dir / target_subdir
            
            if not dry_run and not full_target_dir.exists():
                os.makedirs(full_target_dir, exist_ok=True)
            
            # Move file
            target_path = full_target_dir / file_path.name
            if dry_run:
                print(f"Would {'link' if use_symlinks else 'copy'} {file_path} to {target_path}")
            else:
                if use_symlinks:
                    # Create symbolic link
                    if os.path.exists(target_path) or os.path.islink(target_path):
                        os.unlink(target_path)
                    try:
                        os.symlink(os.path.realpath(file_path), target_path)
                        print(f"Linked {file_path} to {target_path}")
                    except Exception as e:
                        print(f"Failed to create symlink: {e}")
                        continue
                else:
                    # Copy file
                    try:
                        shutil.copy2(file_path, target_path)
                        print(f"Copied {file_path} to {target_path}")
                    except Exception as e:
                        print(f"Failed to copy file: {e}")
                        continue
            
            result[str(full_target_dir)].append(target_path)
        
        return result
    
    def _is_file_type(self, filename: str, file_type: str, component: str) -> bool:
        """
        Check if a filename matches a file type category.
        
        This method can be customized with component-specific logic.
        
        Args:
            filename: The filename to check
            file_type: The file type category (e.g., 'fastq', 'fastqc', 'multiqc')
            component: The component name
            
        Returns:
            True if the filename matches the file type
        """
        # File type detection rules based on component
        if component == "raw_reads":
            if file_type == "raw_fastq" and (filename.endswith('.fastq.gz') or filename.endswith('.fq.gz')):
                return "fastqc" not in filename and "multiqc" not in filename
            elif file_type == "raw_fastqc" and "fastqc" in filename:
                return "multiqc" not in filename
            elif file_type == "raw_multiqc" and "multiqc" in filename:
                return True
        
        # Generic fallback - try to match the file type name in the filename
        return file_type.replace("_", "") in filename.lower()


def move_files(
    assay_type: str,
    component: str,
    files: List[Union[str, Path]],
    output_dir: Union[str, Path],
    dry_run: bool = False,
    use_symlinks: bool = False,
    plugin_dir: Optional[str] = None
) -> Dict[str, List[Path]]:
    """
    Core implementation for organizing files based on component structure.
    
    This function is the main implementation behind the 'dp_tools move' CLI command. It:
    1. Dynamically loads the component module (e.g., dp_tools.assays.rnaseq.raw_reads)
    2. Reads the STRUCTURE dictionary from that module
    3. Determines the correct output directory for each file based on its type
    4. Creates symbolic links or copies files to their target locations
    
    Components define their own structure using a STRUCTURE dictionary that maps
    file types to target directories. For example:
    
    STRUCTURE = {
        "raw_fastq": "01-RawData/Fastq",
        "raw_fastqc": "01-RawData/QC/FastQC",
        "raw_multiqc": "01-RawData/QC/MultiQC"
    }
    
    Args:
        assay_type: The type of assay (e.g., 'rnaseq', 'atacseq')
        component: The component of the assay (e.g., 'raw_reads', 'trimmed_reads')
        files: List of files to organize
        output_dir: Base output directory
        dry_run: If True, only print what would be done
        use_symlinks: If True, create symbolic links instead of copying files
        plugin_dir: Directory containing alternate assay implementation
        
    Returns:
        Dictionary of organized files by target directory
    """
    mover = Mover(assay_type, plugin_dir)
    return mover.move_files(component, files, output_dir, dry_run, use_symlinks)


def stage_files(
    assay_type: str, 
    component: str, 
    output_dir: Union[str, Path],
    plugin_dir: Optional[str] = None,
    **file_paths
) -> Dict[str, List[Path]]:
    """
    Stage files to their proper locations using symbolic links.
    
    This function is a convenience wrapper around move_files that:
    1. Always uses symbolic links
    2. Takes named arguments for file sources by file type
    
    Args:
        assay_type: The type of assay (e.g., 'rnaseq', 'atacseq')
        component: The component of the assay (e.g., 'raw_reads', 'trimmed_reads')
        output_dir: Base output directory
        plugin_dir: Directory containing alternate assay implementation
        **file_paths: Keyword arguments mapping file types to source paths
        
    Returns:
        Dictionary of staged files by target directory
    """
    # Collect all files from the provided paths
    all_files = []
    for file_type, path in file_paths.items():
        if path:
            path = Path(path)
            if path.is_dir():
                # If a directory is provided, include all files in it
                all_files.extend([str(f) for f in path.glob('*') if f.is_file()])
            else:
                # If a single file is provided
                all_files.append(str(path))
    
    # Use move_files with symlinks
    return move_files(
        assay_type=assay_type,
        component=component,
        files=all_files,
        output_dir=output_dir,
        dry_run=False,
        use_symlinks=True,
        plugin_dir=plugin_dir
    ) 