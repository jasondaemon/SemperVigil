from pathlib import Path
import shutil
import subprocess
import yaml

import pytest

pytestmark = pytest.mark.offline


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("value", ["0", "25"])
def test_background_limit_is_rendered(value):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    output = subprocess.check_output([
        "helm", "template", "test", str(chart), "--show-only", "templates/configmap-env.yaml",
        "--set-string", f"env.SV_FEED_ARCHIVE_BACKGROUND_DAYS={value}",
    ], text=True)
    assert f'SV_FEED_ARCHIVE_BACKGROUND_DAYS: "{value}"' in output


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("value", [None, "0", "1", "5"])
def test_builder_override_is_scoped_and_optional(value):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    args = ["helm", "template", "test", str(chart), "--set-string", "env.SV_FEED_ARCHIVE_BACKGROUND_DAYS=0"]
    if value is not None:
        args += ["--set-string", f"buildWorker.feedArchiveBackgroundDays={value}"]
    docs = list(yaml.safe_load_all(subprocess.check_output(args, text=True)))
    for doc in docs:
        if not doc or doc.get("kind") != "Deployment":
            continue
        for container in doc["spec"]["template"]["spec"]["containers"]:
            overrides = [e["value"] for e in container.get("env", []) if e["name"] == "SV_FEED_ARCHIVE_BACKGROUND_DAYS"]
            expected = [value] if value is not None and doc["metadata"]["name"].endswith("build-worker") else []
            assert overrides == expected


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("value", ["-1", "1.5", "invalid", "true"])
def test_invalid_builder_background_limit_fails_render(value):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    result = subprocess.run(["helm", "template", "test", str(chart), "--set-string",
                             f"buildWorker.feedArchiveBackgroundDays={value}"], capture_output=True, text=True)
    assert result.returncode != 0
    assert "must be a non-negative integer or null" in result.stderr
