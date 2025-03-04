"""
A script that converts ISA Archives into a specific format based on a provided configuration file

Optional: Has provisions for adding in extra fields as well
"""
import argparse
from pathlib import Path
import re
from typing import List, Union

from dp_tools.config import schemas
from dp_tools.core.configuration import load_config
from dp_tools.core.files import isa_archive
from dp_tools.core.files.isa_archive import isa_investigation_subtables
from dp_tools.glds_api.commons import retrieve_file_url
from dp_tools import plugin_api


import pandas as pd
from pandera import DataFrameSchema

import os
import sys
import atexit

from loguru import logger as log


class BulkRNASeqMetadataComponent:
    pass


def get_assay_table_path(
    ISAarchive: Path, configuration: dict, return_index: bool = False
) -> tuple[List[Path], List[int]]:
    """Retrieve the assay table file name that determined as a valid assay based on configuration.
    Specifically, defined in subsection 'ISA meta'

    :param study_assay_table: From the investigation file
    :type study_assay_table: pd.DataFrame
    :param configuration: Standard assay parsed config
    :type configuration: dict
    :return: list of paths to the found assay table
    :rtype: Union[List[Path], List[int]]
    """
    config = configuration["ISA Meta"]
    # retrieve study assay subtable from I_file
    df = isa_investigation_subtables(ISAarchive)["STUDY ASSAYS"]

    # get valid tuples of measurement and technology types from configuration
    valid_measurements_and_technology_types: list[tuple[str, str]] = [
        (entry["measurement"], entry["technology"])
        for entry in config["Valid Study Assay Technology And Measurement Types"]
    ]

    # check for matching rows based on configuration tuple

    matches: list[Path] = list()
    match_indices = []  # List to store matching indices

    for valid_combination in valid_measurements_and_technology_types:
        log.debug(f"Searching subtable for {valid_combination}")
        

        mask = (df[["Study Assay Measurement Type", "Study Assay Technology Type"]] == valid_combination).all(axis="columns")
        
        match_row = df[mask]
        match_file = [val for val in match_row["Study Assay File Name"].values]
        matches.extend(match_file)

        # Use mask to get index
        match_index = match_row.index.values
        match_indices.extend(match_index)
    if not matches:
        raise ValueError(f"No matches found for queries: {valid_measurements_and_technology_types} against these listed in the ISA archive: {df[['Study Assay Measurement Type', 'Study Assay Technology Type']]}")

    all_files = {f.name: f for f in isa_archive.fetch_isa_files(ISAarchive)}
    assay_paths = [all_files[match] for match in matches if match in all_files]


    return assay_paths, match_indices


SUPPORTED_CONFIG_TYPES = ["microarray", "bulkRNASeq", "methylSeq", "amplicon", "metagenomics"]


def _parse_args():
    """Parse command line args."""
    parser = argparse.ArgumentParser(
        description=f"Script for downloading latest ISA from GLDS repository"
    )
    parser.add_argument(
        "--accession", metavar="GLDS-001", required=True, help="GLDS accession number"
    )
    parser.add_argument(
        "--config-type",
        required=False,
        help=f"Packaged config type to use. Currently supports: {SUPPORTED_CONFIG_TYPES}",
    )
    parser.add_argument(
        "--config-version", default="Latest", help="Packaged config version to use"
    )
    parser.add_argument(
        "--isa-archive",
        required=True,
        help="Local location of ISA archive file. Can be downloaded from the GLDS repository with 'dpt-get-isa-archive'",
    )
    parser.add_argument(
        "--inject",
        required=False,
        nargs='+',
        default=[],
        help="A set of key value pairs to inject into the runsheet.  Useful to add data that is not present in the ISA archive. Format: 'Column_Name=Value'",
    )
    parser.add_argument(
        "--plugin-dir",
        help=f"Plugin directory to load",
        default=False,
    )

    args = parser.parse_args()
    return args


def main():
    args = _parse_args()

    # Configure logging
    log.remove()  # Remove default handler
    log.add(sys.stderr, level="INFO")  # Add stderr handler with INFO level

    # Ensure we have a valid config
    if args.config_version is None:
        args.config_version = "Latest"

    inject = {key_value_pair.split("=")[0]:key_value_pair.split("=")[1] for key_value_pair in args.inject} # Format key value pairs

    if args.plugin_dir == False:
        assert (
            args.config_type in SUPPORTED_CONFIG_TYPES
        ), f"Invalid config type supplied: '{args.config_type}' Supported config types: {SUPPORTED_CONFIG_TYPES} "
        config = (args.config_type, args.config_version)
        isa_to_runsheet(args.accession, Path(args.isa_archive), config, inject = inject)
    else:
        plugin = plugin_api.load_plugin(Path(args.plugin_dir))
        isa_to_runsheet(
            args.accession, 
            Path(args.isa_archive), 
            config=plugin.config, 
            schema=plugin.schemas.runsheet,
            inject = inject
            )


def get_column_name(df: pd.DataFrame, target: Union[str, list]) -> str:
    try:
        match target:
            case str():
                [target_col] = (col for col in df.columns if col in target)
                return target_col
            case list():
                for query in target:
                    try:
                        [target_col] = (col for col in df.columns if col in query)
                        return target_col
                    except ValueError:
                        continue
                # if this runs, the list did not match anything!
                raise ValueError(
                    f"Could not find required column '{target}' "
                    f"in either ISA sample or assay table. These columns were found: {list(df.columns)}"
                )
    except ValueError as e:
        raise ValueError(
            f"Could not find required column '{target}' "
            f"in either ISA sample or assay table. These columns were found: {list(df.columns)}"
        ) from e


# TODO: Needs heavy refactoring and log messaging
def isa_to_runsheet(accession: str, isaArchive: Path, config: Union[tuple[str, str], Path], inject: dict[str, str] = {}, schema: Union[DataFrameSchema, None] = None, assert_factor_values: bool = True):
    ################################################################
    ################################################################
    # SETUP CONFIG AND INPUT TABLES
    ################################################################
    ################################################################
    log.info("Setting up to generate runsheet dataframe")
    configuration = load_config(config=config)
    if configuration['NAME'] == "amplicon":
            atexit.register(lambda: print("Warning: This script may not work as intended for amplicon sequencing datasets annotated before 2022. The Data Processing Team is actively working to address this issue.", file=sys.stderr))
    R1_designations = ["_R1_", "_R1.", "-R1.", "-R1-", ".R1.", "_1."]
    R2_designations = ["_R2_", "_R2.", "-R2.", "-R2-", ".R2.", "_2."]

    if schema is None:
        runsheet_schema = schemas.runsheet[config[0]]
    else:
        runsheet_schema = schema
    i_tables = isa_investigation_subtables(isaArchive)

    assay_table_paths, assay_table_indices = get_assay_table_path(ISAarchive=isaArchive, configuration=configuration)
    # Check if there are multiple valid assays. If there are, add the assay path to the runsheet file name 
    multiple_valid_assays = len(assay_table_paths) > 1

    # Iterate over all paths and process the data
    final_dfs = []


    # Iterate over all paths and process the data
    for assay_table_path, a_study_assays_index in zip(assay_table_paths, assay_table_indices):
        with open(assay_table_path, 'r') as f:
            first_line = f.readline().strip()

        a_table = pd.read_csv(
            assay_table_path,
            sep="\t",
            dtype=str
        )

        [s_file] = (
            f for f in isa_archive.fetch_isa_files(isaArchive) if f.name.startswith("s_")
        )
        s_table = pd.read_csv(s_file, sep="\t", dtype=str)
        df_merged = s_table.merge(a_table, on="Sample Name").set_index(
            "Sample Name", drop=True
        )

        ################################################################
        ################################################################
        # GENERATE FINAL DATAFRAME
        ################################################################
        ################################################################
        log.info("Generating runsheet dataframe")
        df_final = pd.DataFrame(index=df_merged.index)
        # extract from Investigation table first
        investigation_source_entries = [
            entry
            for entry in configuration["Staging"]["General"]["Required Metadata"][
                "From ISA"
            ]
            if entry["ISA Table Source"] == "Investigation"
        ]
        for entry in investigation_source_entries:
            # handle special cases
            if entry.get("True If Includes At Least One"):
                # Addressing non-uniformity in ISA field naming
                # - remove whitespace before comparing
                # - convert to all lower case as well
                isa_entries = {item.strip().lower() for item in i_tables[entry["Investigation Subtable"]][entry["ISA Field Name"]]}
                overlap = set(entry["True If Includes At Least One"]).intersection(
                    isa_entries
                )
                df_final[entry["Runsheet Column Name"]] = bool(overlap)
                continue

            target_investigation_column = i_tables[entry["Investigation Subtable"]].loc[
                a_study_assays_index
            ]
            df_final[entry["Runsheet Column Name"]] = target_investigation_column[
                entry["ISA Field Name"]
            ]

        # extract from assay table first
        assay_source_entries = [
            entry
            for entry in configuration["Staging"]["General"]["Required Metadata"][
                "From ISA"
            ]
            if entry["ISA Table Source"] in ["Assay", "Sample", ["Assay", "Sample"]]
            and entry.get("Autoload", True) != False
        ]
        for entry in assay_source_entries:
            assert list(df_final.index) == list(df_merged.index)
            use_fallback_value = False
            if entry.get("Runsheet Index"):
                # already set and checked above
                continue
            else:
                # merged sequence data file style extraction
                if entry.get("Multiple Values Per Entry"):
                    # getting compatible column
                    target_col = get_column_name(df_merged, entry["ISA Field Name"])

                    # Splits values on match regex entry.get("Match Regex") for primers - only for Amplicon
                    if entry.get("Match Regex"):
                        try:
                            pattern = entry.get("Match Regex")
                            values: pd.DataFrame = df_merged[target_col].str.extractall(pattern).unstack(level=-1)
                            values.columns = values.columns.droplevel(0)
                        except re.error as e:
                            print(f"Invalid primer regex pattern: {e}")
                        except Exception as e:
                            print(f"An error occurred while trying to find primers: {e}")
                        # If the resulting DataFrame is still empty, copy the (CSV) entries to the primer cols, need to refactor for SE assays
                        if values.empty:
                            values[0] = df_merged[target_col]
                            values[1] = df_merged[target_col]
                    else:
                        # split into separate values based on delimiter
                        values: pd.DataFrame = df_merged[target_col].str.split(
                            pat=entry["Multiple Values Delimiter"], expand=True
                        )

                    # Swap read path columns if necessary
                    if values.shape[1] > 1:
                        # Escape special characters in R2_designations to search for them 
                        R2_pattern = '|'.join([re.escape(designation) for designation in R2_designations])
                        # Creating a mask where True indicates the presence of any 'R2_designations'
                        mask = values[0].str.contains(R2_pattern, na=False, case=False, regex=True)
                        # Only swap values where the mask is True
                        values.loc[mask, [0, 1]] = values.loc[mask, [1, 0]].values

                    # rename columns with runsheet names, checking if optional columns are included
                    runsheet_col: dict
                    for runsheet_col in entry["Runsheet Column Name"]:
                        if runsheet_col["index"] in values.columns:
                            values = values.rename(
                                columns={runsheet_col["index"]: runsheet_col["name"]}
                            )
                        else:  # raise exception if not marked as optional
                            if not runsheet_col["optional"]:
                                raise ValueError(
                                    f"Could not populate runsheet column (config: {runsheet_col}). Data may be missing in ISA or the configuration may be incorrect"
                                )

                    if entry.get("GLDS URL Mapping"):
                        values2 = values.map(
                            lambda filename: retrieve_file_url(
                                accession=accession, filename=filename
                            )
                        )  # inplace operation doesn't seem to work
                    else:
                        values2 = values
                        
                    # Extract suffixes if working on suffix entry - only for Amplicon
                    if 'raw_R1_suffix' in entry["Runsheet Column Name"][0].values():
                        extensions = [".fq", ".fastq", ".fastq.gz", "HRremoved_raw.fastq.gz"]
                        # Convert designations to regex patterns, escape special characters, add extensions, eos character
                        R1_patterns = [re.compile(re.escape(d) + r'[^ ]*(' + '|'.join(extensions) + ')$', re.IGNORECASE) for d in R1_designations]
                        R2_patterns = [re.compile(re.escape(d) + r'[^ ]*(' + '|'.join(extensions) + ')$', re.IGNORECASE) for d in R2_designations]
                        SE_pattern = re.compile(re.escape("_raw") + r'[^ ]*(' + '|'.join(extensions) + ')$', re.IGNORECASE)
                        # Extract suffixes based on designations
                        def extract_suffix(filename):
                            matches = []
                            # Check for R1 designations
                            for pattern in R1_patterns:
                                match = re.search(pattern, filename)
                                if match:
                                    matches.append(match.group())
                            # Check for R2 designations if no R1 designation was found
                            if not matches:
                                for pattern in R2_patterns:
                                    match = re.search(pattern, filename)
                                    if match:
                                        matches.append(match.group())
                            # If neither designations found, assume it's a SE file
                            if not matches:
                                match = re.search(SE_pattern, filename)
                                if match:
                                    matches.append(match.group())

                            # Assert that there's only one match and return it
                            try:
                                [unique_match] = matches  # This will raise an error if there's not exactly one match
                            except ValueError:
                                raise ValueError(f"Expected 1 file suffix but found {len(matches)} found in {filename}.")
                            return unique_match
                        
                        values2 = values2.map(extract_suffix)




                    # add to final dataframe and check move onto entry
                    df_final = df_final.join(values2)
                    continue

                # factor value style extraction
                if entry.get("Matches Multiple Columns") and entry.get("Match Regex"):
                    # find matching columns
                    match_cols = [
                        (i, col, df_merged[col])
                        for i, col in enumerate(df_merged.columns)
                        if re.match(pattern=entry.get("Match Regex"), string=col)
                    ]

                    # check if columns require appending unit
                    if entry.get("Append Column Following"):
                        match_i: int  # index in matching column list
                        df_i: int  # index in merged dataframe
                        col: str
                        original_series: pd.Series
                        for match_i, (df_i, col, original_series) in enumerate(match_cols):
                            # scan through following columns
                            for scan_col in df_merged.iloc[:, df_i+1:].columns:
                                # check if another 'owner' column is scanned, this means Unit was not found
                                if any(
                                    [
                                        scan_col.startswith("Parameter Value["),
                                        scan_col.startswith("Factor Value["),
                                        scan_col.startswith("Characteristics["),
                                    ]
                                ):
                                    break
                                if scan_col.startswith(entry.get("Append Column Following")): # uses startswith to avoid naming issues due to pandas 'mangle_dupe_cols' behavior in read csv
                                    resolved_series = original_series.astype(str) + ' ' + df_merged[scan_col]
                                    match_cols[match_i] = df_i, col, resolved_series
                                    break

                    # finally add this information into dataframe
                    for _, col_name, series in match_cols:
                        df_final[col_name] = series
                else:
                    # CAUTION: normally this wouldn't be safe as the order of rows isn't considered.
                    # In this block, the indices are checked for parity already making this okay
                    if entry.get("Value If Not Found"):
                        try:
                            target_col = get_column_name(df_merged, entry["ISA Field Name"])
                            series_to_add = df_merged[target_col]
                        except ValueError:
                            series_to_add = pd.DataFrame(
                                data={
                                    "FALLING_BACK_TO_DEFAULT": entry.get(
                                        "Value If Not Found"
                                    )
                                },
                                index=df_merged.index,
                            )
                    else:
                        try:
                            target_col = get_column_name(df_merged, entry["ISA Field Name"])
                            series_to_add = df_merged[target_col]
                        except ValueError as e: # Raised when a column is not present
                            if entry.get("Fallback Value"):
                                # Create series of same row length as df_merged
                                series_to_add = pd.Series([entry.get("Fallback Value") for _ in range(len(df_merged))])
                                use_fallback_value = True
                                log.warn(f"Could not find column: {entry['ISA Field Name']}. Using configured fallback value: {entry.get('Fallback Value')}")
                            else:
                                raise(e)
                    if entry.get("GLDS URL Mapping"):
                        def map_url_to_filename(fn: str) -> str:
                            try:
                                return retrieve_file_url(accession=accession, filename=fn)
                            except KeyError:
                                raise ValueError(
                                    f"{fn} does not have an associated url in {urls}"
                                )

                        _swap = series_to_add.map(
                            map_url_to_filename
                        )  # inplace operation doesn't seem to work
                        series_to_add = _swap
                    if use_fallback_value:
                        df_final[entry["Runsheet Column Name"]] = entry["Fallback Value"]
                    elif entry.get("Remapping"):
                        df_final[entry["Runsheet Column Name"]] = series_to_add.map(
                            lambda val: entry.get("Remapping")[val]
                        )
                    else:
                        df_final[entry["Runsheet Column Name"]] = series_to_add
        ################################################################
        ################################################################
        # PREPROCESSING
        # - Create new column
        #   - Original Sample Name (used for post processing consistency)
        # - Reworks Sample Name for processing compatibility
        ################################################################
        ################################################################
        # to preserve the original sample name for post processing
        # make a new column
        df_final["Original Sample Name"] = df_final.index

        # Inject any columns supplied
        for col_name, value in inject.items():
            log.info(f"INJECTION: Column '{col_name}' being set to '{value}'")
            df_final[col_name] = value

        # then modify the index as needed
        df_final.index = df_final.index.str.replace(" ", "_")
        modified_samples: list[str] = list(
            df_final.loc[df_final.index != df_final["Original Sample Name"]].index
        )
        if len(modified_samples) != 0:
            log.info(
                f"The following orignal sample names modified for processing: {modified_samples}"
            )

        # if amplicon runsheet: make groups column
        if configuration['NAME'] == "amplicon":
            factor_value_cols = [col for col in df_final.columns if 'Factor Value' in col]
            df_final['groups'] = df_final[factor_value_cols].apply(lambda row: ' & '.join(row.values.astype(str)), axis=1)

        ################################################################
        ################################################################
        # VALIDATION
        ################################################################
        ################################################################
        # TODO: Need to make the validation generalized, maybe load a validation object based on a configuration key?
        log.info("Validating runsheet dataframe")
        # validate dataframe contents (incomplete but catches most required columns)
        # uses dataframe to dict index format: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html

        runsheet_schema.validate(df_final)

        if assert_factor_values:
            # ensure at least one Factor Value is extracted
            # Ensure that all column names are strings and check if any start with "Factor Value["
            assert (
                len([col for col in df_final.columns if str(col).startswith("Factor Value[")]) != 0
            ), f"Must extract at least one factor value column but only has the following columns: {df_final.columns}"


        ################################################################
        ################################################################
        # WRITE OUTPUT
        ################################################################
        ################################################################
        
        if configuration['NAME'] == "amplicon":
            naming_column = "Library Selection"
        # elif configuration['NAME'] == "metagenomics":
        #     naming_column = "Library Kit"
        else:
            naming_column = None
        
        assay_file_suffix = ""

        if multiple_valid_assays:
            if naming_column:
                # Find columns that contain the substring naming_column
                matching_columns = [col for col in df_final.columns if naming_column.lower() in col.lower()]
                
                if matching_columns:
                    col = matching_columns[0]
                    if naming_column not in col:
                        print(f"Inconsistent naming found in {os.path.basename(assay_table_path)} column: {col}")
                    
                    # Use the value from the first row instead of the column name
                    value = df_final[col].iloc[0] if not df_final.empty else ""
                    if isinstance(value, str) and value.strip():
                        # Create file suffix from the value in the column
                        assay_file_suffix = "_" + value.strip().replace(" ", "_").upper()
                    else:
                        # Fallback if empty or not a string
                        assay_file_suffix = "_" + configuration['NAME']
                else:
                    assay_file_suffix = "_" + configuration['NAME']
            else:
                assay_file_suffix = ""

            assay_table_file = os.path.basename(assay_table_path)
            assay_table_name, _ = os.path.splitext(assay_table_file)
            output_fn = f"{accession}{assay_file_suffix}_{assay_table_name}_{configuration['NAME']}_v{configuration['VERSION']}_runsheet.csv"

        else:
            output_fn = f"{accession}_{configuration['NAME']}_v{configuration['VERSION']}_runsheet.csv"

        # Logging the final output path and DataFrame dimensions
        log.info(f"Writing runsheet to: {output_fn} with {df_final.shape[0]} rows and {df_final.shape[1]} columns")

        # Write the DataFrame to a CSV file
        df_final.to_csv(output_fn)
        final_dfs.append(df_final)

    if len(final_dfs) == 1:
        return final_dfs[0]  # Return the sole dataframe
    else:
        return final_dfs  # Return the list of dataframes


def convert_isa_to_runsheet(accession: str, config_type: str, config_version: str, isa_archive: str, output_dir: str = "."):
    """Converts an ISA archive to a runsheet.
    
    This is the main function exposed to the CLI.
    
    Args:
        accession: GLDS or OSD accession number, e.g., GLDS-194 or OSD-194
        config_type: Packaged config type to use (e.g., bulkRNASeq, microarray)
        config_version: Packaged config version to use (e.g., Latest)
        isa_archive: Path to the ISA archive file
        output_dir: Directory to save the output runsheet to. Defaults to current directory.
    """
    # Configure logging
    log.remove()  # Remove default handler
    log.add(sys.stderr, level="INFO")  # Add stderr handler with INFO level
    
    # Change to output directory
    original_dir = Path.cwd()
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    os.chdir(output_path)
    
    try:
        # Validate config_type
        if config_type not in SUPPORTED_CONFIG_TYPES:
            log.error(f"Invalid config type: {config_type}. Supported types: {SUPPORTED_CONFIG_TYPES}")
            return
        
        # Run the conversion
        config = (config_type, config_version)
        isa_to_runsheet(accession, Path(isa_archive), config)
    finally:
        # Change back to original directory
        os.chdir(original_dir)


if __name__ == "__main__":
    main()
