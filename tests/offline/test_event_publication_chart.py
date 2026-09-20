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


def test_automatic_enrollment_changes_only_orchestrator_credentials_and_image(tmp_path):
    values = tmp_path / 'values.yaml'
    values.write_text(yaml.safe_dump({
        'orchestrator': {'eventAutoScopes': '{"event":"' + 'a'*64 + '"}', 'imageTag': 'auto-test'},
        'eventPublication': {'existingSecret': 'event-roles'},
        'env': {'SV_EVENT_PUBLICATION_ENABLED': '1', 'SV_EVENT_ACTIVATION_CHECK': '1',
                'SV_EVENT_HUMAN_APPROVAL_ENABLED': '1'}}))
    result = subprocess.run(['helm','template','test',str(CHART),'-f',str(values)],capture_output=True,text=True)
    assert result.returncode == 0, result.stderr
    deployments = {d['metadata']['name']:d for d in yaml.safe_load_all(result.stdout)
                   if d and d.get('kind') == 'Deployment'}
    for name, deployment in deployments.items():
        container = deployment['spec']['template']['spec']['containers'][0]
        if name.endswith('-orchestrator'):
            assert container['image'].endswith(':auto-test')
            env = {v['name']:v for v in container['env']}
            assert env['SV_EVENT_APPROVAL_DB_URL']['valueFrom']['secretKeyRef']['key'] == 'SV_EVENT_APPROVAL_DB_URL'
            assert 'SV_EVENT_PROMOTION_DB_URL' not in env
        else:
            assert not container['image'].endswith(':auto-test')


def test_auto_enrollment_without_publication_fails_render():
    result = render('orchestrator.eventAutoScopes=invalid-but-nonempty')
    assert result.returncode != 0 and 'automatic enrollment requires' in result.stderr


def test_living_research_enrollment_is_orchestrator_only(tmp_path):
    baseline = render()
    assert baseline.returncode == 0, baseline.stderr
    values = tmp_path / "values.yaml"
    values.write_text(yaml.safe_dump({"orchestrator": {
        "eventResearchEvents": '["evt_waterplum"]',
        "eventResearchIntervalSeconds": 43200,
        "eventResearchMaxResults": 8,
    }}))
    enrolled = subprocess.run(
        ["helm", "template", "test", str(CHART), "-f", str(values)],
        capture_output=True, text=True,
    )
    assert enrolled.returncode == 0, enrolled.stderr
    before = {doc["metadata"]["name"]: doc for doc in yaml.safe_load_all(baseline.stdout)
              if doc and doc.get("kind") == "Deployment"}
    after = {doc["metadata"]["name"]: doc for doc in yaml.safe_load_all(enrolled.stdout)
             if doc and doc.get("kind") == "Deployment"}
    assert set(before) == set(after)
    changed = []
    for name in before:
        if before[name] != after[name]:
            changed.append(name)
    assert changed == ["sempervigil-orchestrator"]
    container = after[changed[0]]["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item for item in container["env"]}
    assert env["SV_EVENT_RESEARCH_EVENTS"]["value"] == '["evt_waterplum"]'
    assert env["SV_EVENT_RESEARCH_INTERVAL_SECONDS"]["value"] == "43200"
    assert env["SV_EVENT_RESEARCH_MAX_RESULTS"]["value"] == "8"
    assert "SV_EVENT_APPROVAL_DB_URL" not in env
