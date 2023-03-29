import sys
from argparse import ArgumentError, ArgumentParser

import pytest

from pgactivity import cli


@pytest.fixture
def parser() -> ArgumentParser:
    if sys.version_info < (3, 9):
        pytest.skip("requires python 3.9+")
    return ArgumentParser(exit_on_error=False)


def test_as_tuple(parser: ArgumentParser) -> None:
    parser.add_argument("coords", type=cli.as_tuple(float))
    args = parser.parse_args(args=["X:1.23"])
    assert vars(args) == {"coords": ("x", 1.23)}

    with pytest.raises(
        ArgumentError, match="argument coords: invalid convert value: 'oh:eh'"
    ):
        parser.parse_args(args=["oh:eh"])


def test_AppendUniqueKeyAction(parser: ArgumentParser) -> None:
    parser.add_argument(
        "-u", type=cli.as_tuple(float), action=cli.AppendUniqueKeyAction
    )
    with pytest.raises(ArgumentError, match="argument -u: duplicated option -u=x"):
        parser.parse_args(args=["-u", "x:1.23", "-u", "x:4"])

    args = parser.parse_args(args=["-u", "x:1.23", "-u", "y:4"])
    assert vars(args) == {"u": [("x", 1.23), ("y", 4.0)]}
