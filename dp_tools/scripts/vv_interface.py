from pathlib import Path
import json

import click
from loguru import logger
import pandas as pd

from dp_tools.plugin_api import load_plugin
from dp_tools.core.loaders import load_data
from dp_tools.core.check_model import ValidationProtocol, FlagCode, run_manual_check


@click.group()
def cli():
    pass


@click.group()
def validation():
    pass


cli.add_command(validation)


@click.command()
@click.option(
    "--output",
    default="VV_report.tsv",
    help="Name of report output file",
    show_default=True,
)
@click.argument("plug_in_dir")
@click.argument("data_dir")
@click.argument("runsheet_path")
@click.option(
    "--data-asset-key-sets",
    type=click.STRING,
    default=None,
    help="Name of data asset key sets to use. Defaults to use all data asset keys in configuration file.",
    show_default=True,
)
@click.option(
    "--run-components",
    type=click.STRING,
    default=None,
    help="Name of components to run. Defaults to use all components.",
    show_default=True,
)
@click.option(
    "--max-flag-code",
    type=click.INT,
    default=FlagCode.HALT.value,
    help="Maximum flag code. If this is exceeded by any check, an error will be raised. Defaults to level associated with FlagCode.HALT.",
    show_default=True,
)
def run(
    plug_in_dir,
    output,
    data_dir,
    runsheet_path,
    data_asset_key_sets,
    run_components,
    max_flag_code,
):
    plugin = load_plugin(Path(plug_in_dir))
    output = Path(output)
    data_dir = Path(data_dir)
    runsheet_path = Path(runsheet_path)
    click.echo(f"Running validation protocol and outputting report to file: '{output}'")
    datasystem = load_data(
        config=plugin.config,
        root_path=data_dir,
        runsheet_path=runsheet_path,
        key_sets=data_asset_key_sets.split(",")
        if data_asset_key_sets is not None
        else None,
    )

    vp = plugin.protocol.validate(
        datasystem.dataset,
        report_args={"include_skipped": True},
        defer_run=True,
        protocol_args={
            "run_components": run_components.split(",")
            if run_components is not None
            else None
        },
    )

    vp.run()
    report = vp.report(
        include_skipped=False, combine_with_flags=datasystem.dataset.loaded_assets_dicts
    )
    # data assets loading information

    # output default dataframe
    samples = list(datasystem.dataset.samples)
    ValidationProtocol.append_sample_column(report["flag_table"], samples=samples)
    df = report["flag_table"]

    # PREPEND MANUAL_CHECKS_PENDING to log file if appropriate
    count_manual_checks = len(df.loc[df["code"] == FlagCode.MANUAL])
    if count_manual_checks != 0:
        output = f"{output}.MANUAL_CHECKS_PENDING"
        click.echo(f"Found {count_manual_checks} Manual checks pending!")

    df.to_csv(output, sep="\t")
    click.echo(f"Writing results to '{output}'")

    # Raise error if any flag code exceeds max_flag_code
    flagged_messages = "\n".join(
        [msg for msg in df.loc[df["code_level"] >= max_flag_code]["message"]]
    )
    assert (
        df["code_level"].max() < max_flag_code
    ), f"Maximum flag code exceeded: {max_flag_code}. Printing flag messages that caused this halt: {flagged_messages}"


@click.command()
@click.argument("validation_report")
def manual_checks(validation_report):
    click.echo(f"Reviewing pending manual checks")
    validation_report = Path(validation_report)

    click.echo("Reading in report: 'validation_report'")
    df = pd.read_csv(validation_report, sep="\t")

    manual_checks_count = 0
    for _, row in df.iterrows():
        if int(row["code_level"]) == FlagCode.MANUAL.value:
            manual_checks_count += 1

    click.echo(
        f"Found {manual_checks_count} manual checks pending... Starting manual review"
    )

    analyst_id = False
    while not analyst_id:
        analyst_id = input("Enter analyst ID (This will be saved in completed log): ")

    new_rows = list()
    for _, row in df.iterrows():
        logger.debug(f"Processsing: {row}")
        # Pass through if not manual
        if int(row["code_level"]) != FlagCode.MANUAL.value:
            new_rows.append(dict(row))
        else:
            # Manual check
            logger.debug(
                f"""Loading as json string: {row['kwargs'].replace("'",'"')}"""
            )
            result = run_manual_check(**json.loads(row["kwargs"].replace("'", '"')))
            # replace original manual check notice with filled results
            row = dict(row) | result
            row["kwargs"] = str(
                json.loads(row["kwargs"].replace("'", '"')) | {"analyst_ID": analyst_id}
            )
            new_rows.append(row)

    new_df = pd.DataFrame(new_rows)

    click.echo("Completed manual checks")

    output = validation_report.with_suffix("")
    new_df.to_csv(output, index=False, sep="\t")  # Remove ".PENDING_MANUAL_CHECKS"

    click.echo(f"Wrote complete report to '{output}'")


@click.command()
@click.option(
    "--output",
    default="protocol_spec.txt",
    help="Name of specification output file",
    show_default=True,
)
@click.argument("plug_in_dir")
@click.argument("data_dir")
@click.argument("runsheet_path")
@click.option(
    "--data-asset-key-sets",
    type=click.STRING,
    default=None,
    help="Name of data asset key sets to use. Defaults to use all data asset keys in configuration file.",
    show_default=True,
)
@click.option(
    "--run-components",
    type=click.STRING,
    default=None,
    help="Name of components to run. Defaults to use all components.",
    show_default=True,
)
def spec(
    plug_in_dir, output, data_dir, runsheet_path, data_asset_key_sets, run_components
):
    plugin = load_plugin(Path(plug_in_dir))
    output = Path(output)
    data_dir = Path(data_dir)
    runsheet_path = Path(runsheet_path)
    click.echo(
        f"Generating specification of validation protocol and outputting to file: '{output}'"
    )
    datasystem = load_data(
        config=plugin.config,
        root_path=data_dir,
        runsheet_path=runsheet_path,
        key_sets=data_asset_key_sets.split(",")
        if data_asset_key_sets is not None
        else None,
    )

    logger.trace(dir(datasystem.dataset))
    logger.trace(datasystem.dataset.loaded_assets_report.iloc[1]['kwargs'])

    vp: ValidationProtocol = plugin.protocol.validate(
        datasystem.dataset,
        report_args={"include_skipped": True},
        defer_run=True,
        protocol_args={
            "run_components": run_components.split(",")
            if run_components is not None
            else None
        },
    )

    specification = vp.queued_checks(
        long_description=True,
        CHECK_PREFIX="",
        COMPONENT_PREFIX=" ",
        INDENT_CHAR="#",
        WRAP_COMPONENT_NAME_CHAR="",
        include_checks_counters=False,
        include_manual_checks=True,
    )


    with open(output, "w") as f:
        f.write(specification)

    click.echo(f"Saved specification to {output}")


validation.add_command(run)
validation.add_command(manual_checks)
validation.add_command(spec)
