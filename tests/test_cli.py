from typer.testing import CliRunner

from usfin import __version__
from usfin.cli import app


def test_version_prints_package_version():
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.output.strip() == __version__


def test_package_version_matches_metadata():
    from importlib.metadata import version

    assert __version__ == version("usfin")
