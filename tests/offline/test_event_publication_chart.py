import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.offline,
              pytest.mark.skipif(not shutil.which("helm"), reason="Helm unavailable")]
CHART = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"


def render(*settings):
    args = ["helm", "template", "test", str(CHART)]
    for setting in settings:
        args += ["--set-string", setting]
    return subprocess.run(args, capture_output=True, text=True)


@pytest.mark.parametrize("enabled", [False, True])
def test_publication_credentials_are_role_scoped(enabled):
    settings = ["eventPublication.existingSecret=event-roles",
                "env.SV_EVENT_PUBLICATION_ENABLED=1", "env.SV_EVENT_ACTIVATION_CHECK=1",
                "env.SV_EVENT_HUMAN_APPROVAL_ENABLED=1"] if enabled else []
    result = render(*settings)
    assert result.returncode == 0, result.stderr
    found = {}
    for doc in yaml.safe_load_all(result.stdout):
        if not doc or doc.get("kind") != "Deployment":
            continue
        for container in doc["spec"]["template"]["spec"]["containers"]:
            credentials = {entry["name"]: entry for entry in container.get("env", [])
                           if entry["name"].startswith("SV_EVENT_") and entry["name"].endswith("DB_URL")}
            if credentials:
                found[container["name"]] = set(credentials)
                for name, entry in credentials.items():
                    assert entry["valueFrom"]["secretKeyRef"] == {"name": "event-roles", "key": name}
    assert found == ({"sempervigil-admin": {"SV_EVENT_APPROVAL_DB_URL"},
                      "sempervigil-worker-fetch": {"SV_EVENT_PROMOTION_DB_URL"},
                      "sempervigil-build-worker": {"SV_EVENT_ACTIVATION_DB_URL"}} if enabled else {})


@pytest.mark.parametrize("settings,message", [
    (["env.SV_EVENT_PUBLICATION_ENABLED=1"], "requires its activation guard"),
    (["env.SV_EVENT_ACTIVATION_CHECK=1"], "requires eventPublication.existingSecret"),
    (["env.SV_EVENT_HUMAN_APPROVAL_ENABLED=1"], "requires eventPublication.existingSecret"),
    (["env.SV_EVENT_PUBLICATION_ENABLED=true"], "must be the string 0 or 1"),
])
def test_invalid_publication_configuration_refuses_render(settings, message):
    result = render(*settings)
    assert result.returncode != 0 and message in result.stderr
