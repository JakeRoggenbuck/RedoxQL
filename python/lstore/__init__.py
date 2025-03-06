from typing import Final
from .lstore import RDatabase, RTable, hello_from_rust, RTableHandle


def print_logo():
    logo = r"""
    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1
    """

    print(logo)


__all__: Final[list[str]] = [
    "lstore",
]
