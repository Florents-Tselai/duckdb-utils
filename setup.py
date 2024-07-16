import os

from setuptools import setup

VERSION = "0.1.0a1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="duckdb-utils",
    description="",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Florents Tselai",
    url="https://github.com/Florents-Tselai/duckdb-utils",
    entry_points="""
        [console_scripts]
        duckdb-utils=duckdb_utils.cli:cli
    """,
    project_urls={
        "Issues": "https://github.com/Florents-Tselai/duckdb-utils/issues",
        "CI": "https://github.com/Florents-Tselai/duckdb-utils/actions",
        "Changelog": "https://github.com/Florents-Tselai/duckdb-utils/releases",
    },
    license=open("LICENSE").read(),
    version=VERSION,
    packages=["duckdb_utils"],
    install_requires=["setuptools", "pip"]
    + ["sqlite-utils"]
    + ["duckdb"]
    + ["tabulate"]
    + ["click", "click-default-group>=1.2.3"],
    extras_require={
        "test": ["pytest", "pytest-cov", "black", "ruff"]
    },
    python_requires=">=3.7",
)
