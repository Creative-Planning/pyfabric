"""pyfabric — Python libraries for creating and validating Microsoft Fabric items."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("pyfabric")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = ["__version__"]
