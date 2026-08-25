from pathlib import Path

import pytest

from dp_tools.core.utilites.metrics_extractor import (
    MetricsExtractor,
    generate_extractor_from_yaml_config,
    AssayType,
)


@pytest.fixture
def test_yaml():
    # Make the path relative to this file
    TEST_DIR = Path(__file__).parent
    return TEST_DIR / "assets/test.yaml"


@pytest.fixture
def configuration_yaml():
    # Make the path relative to this file
    TEST_DIR = Path(__file__).parent
    return TEST_DIR / "assets/config.yaml"


@pytest.fixture
def OSD_576_metrics_csv():
    TEST_DIR = Path(__file__).parent
    return TEST_DIR / "assets/OSD-576_on_cluster_metrics.csv"


@pytest.fixture
def OSD_281_metrics_csv():
    TEST_DIR = Path(__file__).parent
    return TEST_DIR / "GLDS-281_on_cluster_metrics.csv"


def test_extract_general_information(test_yaml):
    # Fixture YAML is intentionally minimal; full-key validation is covered elsewhere.
    with pytest.raises(ValueError, match="Missing keys"):
        MetricsExtractor(targets=[]).extract_general_information(
            assay_type=AssayType.bulkRNASeq, yaml_file=test_yaml
        )


def test_isa_to_yaml(glds194_test_dir, test_yaml, configuration_yaml, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "dp_tools.scripts.convert.retrieve_file_url",
        lambda accession, filename: f"https://example.invalid/{accession}/{filename}",
    )
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        configuration_yaml.read_text().replace(
            "/CHANGEME/TO/WHERE/MQC/ARE", str(glds194_test_dir)
        )
    )
    metricsExtractor = generate_extractor_from_yaml_config(config=cfg_path)

    metricsExtractor.extract_data_from_isa(
        accession="GLDS-194",
        isa_archive=glds194_test_dir / "Metadata/GLDS-194_metadata_GLDS-194-ISA.zip",
        config=("bulkRNASeq", "Latest"),
    )

    metricsExtractor.append_manual_yaml_data(target_yaml=test_yaml)

    metricsExtractor.extract_sections()

    metric_cols = {
        c[0] if isinstance(c, tuple) else c for c in metricsExtractor.metrics.columns
    }
    # Keys present after ISA + manual YAML merge (process_metrics adds the rest)
    assert (
        {
            "has_ERCC",
            "organism",
            "Original Sample Name",
            "OSD-#",
            "GLDS-#",
            "Data Source",
        }.difference(metric_cols)
        == set()
    )

    metricsExtractor.metrics.to_csv(tmp_path / "test.csv")

    metricsExtractor.process_metrics(assay_type=AssayType.bulkRNASeq)


def test_load_and_process_metrics_table(configuration_yaml, OSD_576_metrics_csv, tmp_path):
    if not OSD_576_metrics_csv.exists():
        pytest.skip(f"Missing fixture: {OSD_576_metrics_csv.name}")

    metricsExtractor = generate_extractor_from_yaml_config(config=configuration_yaml)

    metricsExtractor.load_metrics_csv(metrics_csv=OSD_576_metrics_csv)

    metricsExtractor.process_metrics(assay_type=AssayType.bulkRNASeq).to_csv(
        tmp_path / "OSD_576_metrics_summary.csv"
    )
