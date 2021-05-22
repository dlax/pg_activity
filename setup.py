import pathlib
import shutil
import subprocess
import sys
from datetime import date
from distutils.command.build import build as _build

from setuptools import find_packages, setup

HERE = pathlib.Path(__file__).parent

long_description = (HERE / "README.md").read_text()


def get_version() -> str:
    fpath = HERE / "pgactivity" / "__init__.py"
    with fpath.open() as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split('"')[1]
    raise Exception(f"version information not found in {fpath}")


version = get_version()
description = "Command line tool for PostgreSQL server activity monitoring."


class build(_build):
    def run(self):
        super().run()
        build_manpage()


def build_manpage() -> None:
    manpath = HERE / "docs" / "man"
    pod2man = shutil.which("pod2man")
    if pod2man is None:
        print("warning: pod2man not found, skipping man page build", file=sys.stderr)
        return
    print("building man page")
    args = [
        pod2man,
        "-r",
        f"pg_activity {version}",
        "-d",
        date.today().isoformat(),
        "-c",
        description,
        str(manpath / "pg_activity.pod"),
    ]
    r = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    r.check_returncode()
    (manpath / "pg_activity.1").write_bytes(r.stdout)


setup(
    name="pg_activity",
    version=version,
    author="Dalibo",
    author_email="contact@dalibo.com",
    packages=find_packages("."),
    include_package_data=True,
    url="https://github.com/dalibo/pg_activity",
    license="PostgreSQL",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Environment :: Console :: Curses",
        "License :: OSI Approved :: PostgreSQL License",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
    ],
    keywords="postgresql activity monitoring cli sql top",
    cmdclass={
        "build": build,
    },
    python_requires=">=3.6",
    install_requires=[
        "attrs >= 17, !=21.1",
        "blessed",
        "humanize",
        "psutil >= 2.0.0",
    ],
    extras_require={
        "dev": [
            "black",
            "check-manifest",
            "flake8",
            "mypy",
        ],
        "testing": [
            "psycopg2-binary >= 2.8",
            "pytest",
            "pytest-postgresql",
        ],
    },
    data_files=[
        ("share/man/man1", ["docs/man/pg_activity.1"]),
    ],
    entry_points={
        "console_scripts": [
            "pg_activity=pgactivity.cli:main",
        ],
    },
    zip_safe=False,
)
