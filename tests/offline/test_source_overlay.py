import importlib.util
import io
import json
from pathlib import Path
import tarfile

import pytest

pytestmark = pytest.mark.offline
spec = importlib.util.spec_from_file_location(
    "source_overlay", Path(__file__).resolve().parents[2] / "tools/source-overlay.py")
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


def add(archive, name, data):
    member = tarfile.TarInfo(name)
    member.size = len(data)
    archive.addfile(member, io.BytesIO(data))


def test_overlay_preserves_runtime_and_dependencies(tmp_path):
    base, source, output = [tmp_path / name for name in ("base.tar", "source.tar", "output.tar")]
    config = {"config": {"Env": ["PYTHONPATH=/app/src"], "Cmd": ["worker"],
                         "Labels": {"org.opencontainers.image.revision": "original"}},
              "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "a" * 64]}}
    with tarfile.open(base, "w") as archive:
        config_data = tool.encoded(config)
        config_digest = tool.add_blob(archive, config_data)
        manifest = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "config": {"digest": config_digest, "size": len(config_data)},
                    "layers": [{"digest": "sha256:" + "b" * 64, "size": 100}]}
        manifest_data = tool.encoded(manifest)
        manifest_digest = tool.add_blob(archive, manifest_data)
        add(archive, "index.json", tool.encoded({"schemaVersion": 2, "manifests": [{
            "digest": manifest_digest, "size": len(manifest_data), "mediaType": manifest["mediaType"]}]}))
        add(archive, "oci-layout", b'{"imageLayoutVersion":"1.0.0"}')
    with tarfile.open(source, "w") as archive:
        add(archive, "src/sempervigil/example.py", b"value = 1\n")
        add(archive, "docs/not-runtime.md", b"not copied")
    result = tool.overlay(base, source, output, "example:new", "new")
    assert result["base_revision"] == "original"
    with tarfile.open(output) as archive:
        index = json.load(archive.extractfile("index.json"))
        descriptor = index["manifests"][0]
        assert descriptor["annotations"]["io.containerd.image.name"] == "example:new"
        manifest = json.load(archive.extractfile("blobs/sha256/" + descriptor["digest"].split(":")[1]))
        updated = json.load(archive.extractfile("blobs/sha256/" + manifest["config"]["digest"].split(":")[1]))
        assert updated["config"]["Env"] == config["config"]["Env"]
        assert updated["config"]["Cmd"] == config["config"]["Cmd"]
        assert updated["config"]["Labels"]["org.opencontainers.image.revision"] == "new"
        assert updated["rootfs"]["diff_ids"][:-1] == config["rootfs"]["diff_ids"]
        assert manifest["layers"][:-1] == [{"digest": "sha256:" + "b" * 64, "size": 100}]
        with tarfile.open(fileobj=io.BytesIO(archive.extractfile(
                "blobs/sha256/" + manifest["layers"][-1]["digest"].split(":")[1]).read())) as layer:
            assert layer.getnames() == ["app/src/.wh..wh..opq", "app/src/sempervigil/example.py"]
