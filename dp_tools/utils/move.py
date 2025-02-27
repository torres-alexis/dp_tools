"""
File organization utility that uses component-specific layout definitions.

This module uses plugin-specific layout definitions to organize files 
into their correct locations for different components. The plugin-based architecture
allows for flexible organization of files based on configurable layouts defined in
either Python modules or YAML configuration files.
"""
import os
import sys
import shutil
import importlib
import glob
import yaml
from pathlib import Path
from typing import Dict, List, Union, Optional, Any, Callable


class Mover:
    """
    File organization utility that uses component-specific layout definitions.
    
    This class loads plugin components dynamically and uses their defined structure
    to organize files into the appropriate locations. It supports loading component
    definitions from Python modules or from structure definitions in config.yaml files.
    """
    
    def __init__(self, plugin_dir: str):
        """
        Initialize mover for a specific plugin.
        
        Args:
            plugin_dir: Directory containing plugin implementation with component modules
                        and/or config.yaml
        """
        self.plugin_dir = plugin_dir
        self.plugin_name = os.path.basename(os.path.normpath(plugin_dir))
        self.component_modules = {}
    
    def _load_component_module(self, component: str):
        """
        Load a component module from the plugin directory.
        
        This method attempts to:
        1. Import a Python module with the component name from the plugin directory
        2. If that fails, look for a config.yaml file with STRUCTURE definitions
        
        Args:
            component: The component name (e.g., 'raw_reads', 'trimmed_reads')
            
        Returns:
            The loaded module or a MockModule with STRUCTURE from config.yaml,
            or None if neither is found
        """
        try:
            # Add plugin path to sys.path if not already there
            plugin_path = os.path.abspath(self.plugin_dir)
            if plugin_path not in sys.path:
                sys.path.insert(0, plugin_path)
            
            # Try to load from plugin directory
            module_path = f"{component}"
            component_module = importlib.import_module(module_path)
            return component_module
            
        except ImportError as e:
            # If the specific component module doesn't exist
            # Check if there's a config.yaml with a STRUCTURE definition
            yaml_path = os.path.join(self.plugin_dir, "config.yaml")
            if os.path.exists(yaml_path):
                try:
                    with open(yaml_path, 'r') as yaml_file:
                        config = yaml.safe_load(yaml_file)
                        if "STRUCTURE" in config:
                            # Create a mock module with the STRUCTURE attribute
                            class MockModule:
                                STRUCTURE = config["STRUCTURE"]
                            return MockModule()
                except Exception as yaml_e:
                    print(f"Error loading config.yaml: {yaml_e}")
                    
            print(f"Could not load component module '{component}' from plugin '{self.plugin_name}': {e}")
            return None

    def move_files(self, 
              component: str, 
              files: List[Union[str, Path]], 
              output_dir: Union[str, Path],
              dry_run: bool = False,
              use_symlinks: bool = False) -> Dict[str, List[Path]]:
        """
        Move files to their appropriate locations based on the component structure.
        
        This method:
        1. Loads the component module to get the file organization structure
        2. Determines the appropriate destination for each file based on its type
        3. Creates the necessary directory structure
        4. Either copies files or creates symbolic links as specified
        
        Args:
            component: Component name (e.g., 'raw_reads', 'trimmed_reads')
            files: List of files to organize
            output_dir: Base output directory
            dry_run: If True, don't actually move files, only show what would be done
            use_symlinks: If True, create symlinks instead of copying files
            
        Returns:
            Dictionary mapping output directories to lists of moved files
            
        Raises:
            ValueError: If the component module can't be loaded or lacks a STRUCTURE
        """
        # Load the component module
        if component not in self.component_modules:
            component_module = self._load_component_module(component)
            if not component_module:
                raise ValueError(f"Could not find component '{component}' for plugin '{self.plugin_name}'")
            self.component_modules[component] = component_module
        else:
            component_module = self.component_modules[component]
        
        # Get the structure from the module
        if not hasattr(component_module, 'STRUCTURE'):
            raise ValueError(f"Component module '{component}' does not define a STRUCTURE")
        
        structure = component_module.STRUCTURE
        
        # Ensure output_dir is a Path
        output_dir = Path(output_dir)
        
        # Create a map to track which files go where
        file_map = {}
        
        # Go through each file and determine its destination
        for file_path in files:
            file_path = Path(file_path)
            
            # Skip non-existent files
            if not file_path.exists():
                print(f"Warning: File does not exist: {file_path}")
                continue
            
            # Find the right place for this file
            for assay_name, assay_struct in structure.items():
                if assay_name != self.plugin_name and assay_name != '*':
                    continue
                    
                for seq_type, seq_struct in assay_struct.items():
                    for comp_name, comp_struct in seq_struct.get('components', {}).items():
                        if comp_name != component:
                            continue
                            
                        outputs = comp_struct.get('outputs', {})
                        
                        for file_type, rel_dir in outputs.items():
                            if self._is_file_type(file_path.name, file_type, component):
                                target_dir = output_dir / rel_dir
                                
                                if target_dir not in file_map:
                                    file_map[target_dir] = []
                                    
                                file_map[target_dir].append(file_path)
                                
                                # Only map each file once
                                break
        
        # Create result tracker
        result = {}
        
        # Process the file map
        for target_dir, files_to_move in file_map.items():
            # Create directory if needed and not a dry run
            if not dry_run:
                os.makedirs(target_dir, exist_ok=True)
                
            # Track moved files in result
            result[target_dir] = []
                
            # Process each file
            for file_path in files_to_move:
                target_path = target_dir / file_path.name
                
                # Print action
                if dry_run:
                    if use_symlinks:
                        print(f"Would symlink: {file_path} -> {target_path}")
                    else:
                        print(f"Would copy: {file_path} -> {target_path}")
                else:
                    if use_symlinks:
                        # Create a symbolic link
                        if target_path.exists():
                            if target_path.is_symlink():
                                target_path.unlink()
                            else:
                                print(f"Error: Target exists and is not a symlink: {target_path}")
                                continue
                                
                        target_path.symlink_to(file_path.absolute())
                        print(f"Created symlink: {file_path} -> {target_path}")
                    else:
                        # Copy the file
                        shutil.copy2(file_path, target_path)
                        print(f"Copied: {file_path} -> {target_path}")
                
                # Add to result
                result[target_dir].append(target_path)
                    
        return result

    def _is_file_type(self, filename: str, file_type: str, component: str) -> bool:
        """
        Check if a file matches a given file type.
        
        This method uses either:
        1. A custom is_file_type method in the component module if available
        2. Default file type detection based on naming conventions
        
        Args:
            filename: Name of the file
            file_type: Type to check against
            component: Component name (for component-specific detectors)
            
        Returns:
            True if the file is of the specified type, False otherwise
        """
        # Check if the component module has a custom detector
        component_module = self.component_modules.get(component)
        if component_module and hasattr(component_module, 'is_file_type'):
            return component_module.is_file_type(filename, file_type)
            
        # Default file type detection based on naming convention
        # This can be very simplistic and would need to be enhanced
        if file_type == 'raw_fastq' and (filename.endswith('.fastq') or filename.endswith('.fastq.gz')):
            return True
        elif file_type == 'raw_fastqc' and 'fastqc' in filename.lower() and (filename.endswith('.html') or filename.endswith('.zip')):
            return True
        elif file_type == 'raw_multiqc' and 'multiqc' in filename.lower():
            return True
            
        # No match
        return False


def move_files(
    plugin_dir: str,
    component: str,
    files: List[Union[str, Path]],
    output_dir: Union[str, Path],
    dry_run: bool = False,
    use_symlinks: bool = False
) -> Dict[str, List[Path]]:
    """
    Move files to their appropriate locations based on the component structure.
    
    This function is a convenience wrapper around the Mover class that:
    1. Creates a Mover instance for the specified plugin
    2. Calls the move_files method to organize files
    
    Args:
        plugin_dir: Plugin directory containing structure definitions
        component: Component name (e.g., 'raw_reads', 'trimmed_reads')
        files: List of files to organize
        output_dir: Base output directory
        dry_run: If True, don't actually move files, only show what would be done
        use_symlinks: If True, create symlinks instead of copying files
        
    Returns:
        Dictionary mapping output directories to lists of moved files
    """
    # Create a mover for the specified assay type
    mover = Mover(plugin_dir)
    
    # Move the files
    return mover.move_files(
        component=component,
        files=files,
        output_dir=output_dir,
        dry_run=dry_run,
        use_symlinks=use_symlinks
    )


def stage_files(
    plugin_dir: str, 
    component: str, 
    output_dir: Union[str, Path],
    **file_paths
) -> Dict[str, List[Path]]:
    """
    Stage files for processing based on a component's structure.
    
    This is a convenience wrapper around move_files that:
    1. Accepts a mapping of file types to file paths
    2. Handles various input formats (lists, directories, glob patterns)
    3. Always uses symbolic links for staging
    
    Args:
        plugin_dir: Plugin directory containing structure definitions
        component: Component name (e.g., 'raw_reads', 'trimmed_reads')
        output_dir: Base output directory
        **file_paths: Mapping of file types to file paths (can be files, dirs or globs)
        
    Returns:
        Dictionary mapping output directories to lists of moved files
    """
    # Collect all files
    files = []
    for file_type, path in file_paths.items():
        if isinstance(path, list):
            # If it's a list of paths
            files.extend([Path(p) for p in path])
        elif os.path.isdir(path):
            # If it's a directory, include all files in it
            files.extend([Path(path) / f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])
        elif os.path.isfile(path):
            # If it's a single file
            files.append(Path(path))
        elif '*' in path:
            # If it's a glob pattern
            matching_files = glob.glob(path)
            files.extend([Path(f) for f in matching_files])
    
    # Move the files
    return move_files(
        plugin_dir=plugin_dir,
        component=component,
        files=files,
        output_dir=output_dir,
        use_symlinks=True
    ) 