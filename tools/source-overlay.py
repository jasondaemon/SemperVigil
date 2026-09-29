#!/usr/bin/env python3
"""Append an exact source layer to an exported single-platform OCI image.

No dependency installation or daemon is required. The caller must verify the base
image and import the resulting archive into the deployment container runtime.
"""
import argparse
import hashlib
import io
import json
import tarfile
from datetime import datetime, timezone


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def add_blob(archive, data):
    name = "blobs/sha256/" + digest(data).split(":")[1]
    member = tarfile.TarInfo(name)
    member.size = len(data)
    member.mode = 0o644
    archive.addfile(member, io.BytesIO(data))
    return digest(data)


def source_layer(source):
    stream = io.BytesIO()
    count = 0
    with tarfile.open(source) as archive, tarfile.open(fileobj=stream, mode="w") as layer:
        opaque = tarfile.TarInfo("app/src/.wh..wh..opq")
        opaque.mode = 0o644
        layer.addfile(opaque, io.BytesIO())
        for member in archive:
            if not member.name.startswith("src/"):
                continue
            if ".." in member.name.split("/") or not (member.isfile() or member.isdir()):
                raise ValueError("unexpected source archive member")
            member.name = "app/" + member.name
            member.uid = member.gid = 0
            member.uname = member.gname = ""
            layer.addfile(member, archive.extractfile(member) if member.isfile() else None)
            count += int(member.isfile())
    if not count:
        raise ValueError("source archive contains no files")
    return stream.getvalue()


def overlay(base, source, output, name, revision):
    def read_blob(archive, reference):
        algorithm, value = reference.split(":")
        if algorithm != "sha256":
            raise ValueError("unsupported digest")
        data = archive.extractfile("blobs/sha256/" + value).read()
        if digest(data) != reference:
            raise ValueError("base blob digest mismatch")
        return data

    layer = source_layer(source)
    with tarfile.open(base) as archive:
        index = json.load(archive.extractfile("index.json"))
        if len(index["manifests"]) != 1:
            raise ValueError("base must have one platform manifest")
        descriptor = index["manifests"][0]
        manifest = json.loads(read_blob(archive, descriptor["digest"]))
        if "config" not in manifest:
            raise ValueError("export a single-platform image, not a manifest list")
        config = json.loads(read_blob(archive, manifest["config"]["digest"]))
        base_revision = config.get("config", {}).get("Labels", {}).get("org.opencontainers.image.revision")
        if not base_revision:
            raise ValueError("base lacks source revision provenance")
        config.setdefault("config", {}).setdefault("Labels", {})["org.opencontainers.image.revision"] = revision
        config["rootfs"]["diff_ids"].append(digest(layer))
        config.setdefault("history", []).append({
            "created": datetime.now(timezone.utc).isoformat(),
            "created_by": "SemperVigil verified source-only OCI overlay " + revision,
        })
        config_data = encoded(config)
        manifest["config"].update(digest=digest(config_data), size=len(config_data))
        manifest["layers"].append({"mediaType": "application/vnd.oci.image.layer.v1.tar",
                                   "digest": digest(layer), "size": len(layer)})
        manifest_data = encoded(manifest)
        descriptor.update(digest=digest(manifest_data), size=len(manifest_data))
        descriptor["annotations"] = {"io.containerd.image.name": name,
                                     "org.opencontainers.image.ref.name": name}
        with tarfile.open(output, "w") as target:
            for member in archive:
                if member.name in {"index.json", "manifest.json"}:
                    continue
                target.addfile(member, archive.extractfile(member) if member.isfile() else None)
            for data in (layer, config_data, manifest_data):
                add_blob(target, data)
            index_data = encoded(index)
            member = tarfile.TarInfo("index.json")
            member.size = len(index_data)
            target.addfile(member, io.BytesIO(index_data))
    return {"name": name, "revision": revision, "base_revision": base_revision,
            "manifest_digest": digest(manifest_data), "layer_bytes": len(layer)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("base", "source", "output", "name", "revision"):
        parser.add_argument("--" + flag, required=True)
    print(json.dumps(overlay(**vars(parser.parse_args()))))
