from typing import Final
from .lstore import hello_from_rust


msg = hello_from_rust()
assert "Rust" in msg
print(msg)


__all__: Final[list[str]] = [
    "lstore",
]
