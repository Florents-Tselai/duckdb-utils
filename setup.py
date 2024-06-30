from setuptools import setup
import os

VERSION = "0.0.0a0"

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
        duckdb-utils=duckdb-utils.cli:cli
    """,
    project_urls={
        "Issues": "https://github.com/Florents-Tselai/duckdb-utils/issues",
        "CI": "https://github.com/Florents-Tselai/duckdb-utils/actions",
        "Changelog": "https://github.com/Florents-Tselai/duckdb-utils/releases",
    },
    license="MIT License",
    version=VERSION,
    packages=["duckdb_utils"],
    install_requires=[ "click", "setuptools", "pip"],
    extras_require={"test": ["pytest", "pytest-cov", "black", "ruff", "click"]},
    python_requires=">=3.7"
)
