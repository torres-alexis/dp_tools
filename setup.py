#!/usr/bin/env python
import codecs
import os
from setuptools import setup, find_packages

dirname = os.path.dirname(__file__)

# Check if CHANGELOG.md exists
changelog_path = os.path.join(dirname, "CHANGELOG.md")
if os.path.exists(changelog_path):
    long_description = (
        codecs.open(os.path.join(dirname, "README.md"), encoding="utf-8").read()
        + "\n"
        + codecs.open(changelog_path, encoding="utf-8").read()
    )
else:
    long_description = codecs.open(os.path.join(dirname, "README.md"), encoding="utf-8").read()

setup(
    name="dp_tools",
    version="2.0.0",
    description="Tooling for Data Processing Operations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Jonathan Oribello",
    author_email="jonathan.d.oribello@gmail.com",
    packages=find_packages(),
    scripts=[],
    package_data={
        "dp_tools": ["config/*.yaml", "assets/*"],
    },
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "requests>=2.32.3",
        "pyyaml>=6.0.2",
        "pandas>=2.2.03",
        "schema",
        "tabulate",
        "multiqc>=1.27.1",
        "pandera",
        "click>=8.1.0",
        "loguru",
        "networkx>=3.3",
        "numpy>=2.2.3",
        "matplotlib>=3.10.0",
        "seaborn>=0.13.2",
    ],
    setup_requires=[],
    tests_require=["pytest", "pytest-console_scripts"],
    entry_points={
        "console_scripts": [
            "dp_tools=dp_tools.cli.main:dp_tools",
        ]
    }
) 