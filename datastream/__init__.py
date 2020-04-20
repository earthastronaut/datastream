import os

from . import (
    database,
    config,
    consumer,
    producer,
)
from .database import *
from .consumer import *
from .producer import *


__all__ = (
    database.__all__
    + consumer.__all__
    + producer.__all__
)


with open(os.path.join(os.path.dirname(__file__), '__version__')) as _f:
    __version__ = _f.read().rstrip()
