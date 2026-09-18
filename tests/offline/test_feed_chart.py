from pathlib import Path
import shutil
import subprocess

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
