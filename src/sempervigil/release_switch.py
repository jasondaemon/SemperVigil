"""Small filesystem primitive, invoked inside the guarded activation window."""
import os
from pathlib import Path
import sys
import uuid


def atomic_switch(release: Path, current: Path) -> None:
    release, current = release.absolute(), current.absolute()
    if (current.name != "current" or release.parent != current.parent / "releases"
            or release.is_symlink() or not (release / "index.html").is_file()):
        raise ValueError("invalid_release_switch")
    temporary = current.parent / (".current-" + uuid.uuid4().hex)
    os.symlink("releases/" + release.name, temporary)
    try:
        # Same-directory rename replaces the link without an absent-name window.
        os.replace(temporary, current)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("release and current paths required")
    atomic_switch(Path(sys.argv[1]), Path(sys.argv[2]))
