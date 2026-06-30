"""
Data processing tools for GeneLab workflows.

Framework overview:

  Validation (core.check_model)
    ValidationProtocol, FlagCode, run_manual_check. Define checks that return
    pass/fail/flag codes; run protocols over datasets; support manual review.

  Data model (core.entity_model, core.loaders)
    Dataset → Group → Sample hierarchy. DataSystem from runsheet; load_data()
    loads config + runsheet + data assets (paths, MultiQC, etc.) by key.

  Config (config.interface, core.configuration)
    YAML configs per assay (e.g. bulkRNASeq_vLatest). Define data asset
    templates, required metadata, validation components.

  GLDS/OSD API (glds_api.commons, glds_api.isa)
    get_table_of_files, retrieve_file_url, find_matching_filenames.
    Download ISA archives, map GLDS↔OSD accessions.

  Assay plugins (bulkRNASeq, etc.)
    Assay-specific configs, check functions, validation protocols.
    Plugins loaded by validation CLI via plugin_api.

See README for installation and CLI usage.
"""

__version__ = "1.3.12"
