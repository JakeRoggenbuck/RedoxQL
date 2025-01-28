from typing import Final
from .lstore import hello_from_rust, Database

print(Database.ping())

msg = hello_from_rust()
assert "Rust" in msg
print(msg)


__all__: Final[list[str]] = [
    "lstore",
]
