import compileall
import pathlib


def test_compileall_src():
    src_dir = pathlib.Path(__file__).resolve().parents[1] / "src"
    assert src_dir.exists()
    ok = compileall.compile_dir(str(src_dir), quiet=1)
    assert ok, "compileall failed for src/"
