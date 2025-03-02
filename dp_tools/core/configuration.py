import os
from pathlib import Path
from typing import Union
import importlib.resources as pkg_resources
from warnings import warn
from loguru import logger as log

import yaml


def load_full_config(config: Union[str, Path]) -> dict:
    warn(
        "Calls to this function should be migrated to 'load_config'; version=2.0.0",
        DeprecationWarning,
        stacklevel=2,
    )
    if isinstance(config, str):
        resolved_config_path = os.path.join(
            "..", "config", f"bulkRNASeq_v{config}.yaml"
        )
        log.info(f"Loading full config (relative to package): {resolved_config_path}")
        conf_full = yaml.safe_load(
            pkg_resources.files(__name__).joinpath(resolved_config_path).read_bytes()
        )
    elif isinstance(config, Path):
        log.info(f"Loading config (direct path): {config}")
        conf_full = yaml.safe_load(config.open())

    # validate with schema
    # config_schema = Schema()

    log.debug(f"Final config loaded: {conf_full}")

    return conf_full


def load_config(config: Union[tuple[str, str], Path]) -> dict:
    """Load yaml configuration file. Allows loading from either:
      - A prepackaged configuration file using a tuple of ('config_type','config_version') (e.g. ('bulkRNASeq','Latest'), ('microarray','0'))
      - A configuration file supplied as a Path object

    :param config: Configuration file to load
    :type config: Union[tuple[str,str], Path]
    :return: A dictionary of the full configuration
    :rtype: dict
    """
    match config:
        case tuple():
            conf_type, conf_version = config
            resolved_config_path = os.path.join(
                "..", "config", f"{conf_type}_v{conf_version}.yaml"
            )
            log.info(f"Loading config (relative to package): {resolved_config_path}")
            conf_full = yaml.safe_load(
                pkg_resources.files(__name__).joinpath(resolved_config_path).read_bytes()
            )
        case Path():
            log.info(f"Loading config (direct path): {config}")
            conf_full = yaml.safe_load(config.open())
        case _:
            raise ValueError(f"Cannot load config from {config}")

    log.debug(f"Final config loaded: {conf_full}")

    return conf_full


def available_data_asset_keys(config: Union[tuple[str, str], Path]) -> set[str]:
    return set(load_config(config)["data assets"])
