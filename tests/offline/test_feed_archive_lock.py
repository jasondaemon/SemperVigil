import fcntl

import pytest

from sempervigil.worker import _feed_archive_lock

pytestmark = pytest.mark.offline


def test_archive_lock_excludes_other_handles_and_releases_after_error(tmp_path):
    archive = tmp_path / "shared" / "feed"
    with pytest.raises(RuntimeError):
        with _feed_archive_lock(archive):
            with (archive / ".export.lock").open("a") as other:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            raise RuntimeError("serialization failed")
    with (archive / ".export.lock").open("a") as other:
        fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(other.fileno(), fcntl.LOCK_UN)
