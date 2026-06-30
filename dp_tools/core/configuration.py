import os
from pathlib import Path
from typing import Union
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
        # Get path to config directory relative to this module
        config_dir = Path(__file__).parent.parent / "config"
        config_path = config_dir / f"bulkRNASeq_v{config}.yaml"
        log.info(f"Loading full config: {config_path}")
        conf_full = yaml.safe_load(config_path.read_text())
    elif isinstance(config, Path):
        log.info(f"Loading config (direct path): {config}")
        conf_full = yaml.safe_load(config.open())

    # validate with schema
    # config_schema = Schema()

    log.debug(f"Final config loaded: {conf_full}")

    return conf_full


def load_config(config: Union[tuple[str, str], Path]) -> dict:
    """Load yaml configuration file. Allows loading from either:
      - A prepackaged configuration file using a tuple of ('config_type','config_version') (e.g. ('bulkRNASeq','Latest'), ('microarray_agilent','Latest'))
      - A configuration file supplied as a Path object

    :param config: Configuration file to load
    :type config: Union[tuple[str,str], Path]
    :return: A dictionary of the full configuration
    :rtype: dict
    """
    match config:
        case tuple():
            conf_type, conf_version = config
            # Get path to config directory relative to this module
            config_dir = Path(__file__).parent.parent / "config"
            config_path = config_dir / f"{conf_type}_v{conf_version}.yaml"
            log.info(f"Loading config: {config_path}")
            conf_full = yaml.safe_load(config_path.read_text())
        case Path():
            log.info(f"Loading config (direct path): {config}")
            conf_full = yaml.safe_load(config.open())
        case _:
            raise ValueError(f"Cannot load config from {config}")

    log.debug(f"Final config loaded: {conf_full}")

    return conf_full


def available_data_asset_keys(config: Union[tuple[str, str], Path]) -> set[str]:
    return set(load_config(config)["data assets"])
