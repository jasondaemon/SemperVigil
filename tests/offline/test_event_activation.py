import copy
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

from sempervigil import event_activation as activation
from sempervigil.investigation import _version
from test_event_qualified_render import approved_fixture
from test_event_review import database

pytestmark = pytest.mark.offline


def manifest(revisions=None, withdrawn=None):
    return {"workflow": "event-release-authorization-v1",
            "revisions": revisions or {}, "withdrawn": withdrawn or {}}


class Connection:
    autocommit = False
    info = SimpleNamespace(transaction_status=psycopg.pq.TransactionStatus.IDLE)

    def __init__(self, pointers, rows):
        self.pointers, self.rows = pointers, rows
        self.statements = []
        self.open = False
        self.guarded = True
        self.writable = False

    def __enter__(self):
        self.open = True
        return self

    def __exit__(self, *args):
        self.open = False

    def execute(self, query, args=None):
        self.statements.append(query)
        if "has_table_privilege" in query:
            return Mock(fetchone=lambda: (self.writable,))
        if "SELECT event_id,revision_id" in query:
            return Mock(fetchall=lambda: self.pointers)
        if "SELECT EXISTS" in query:
            return Mock(fetchone=lambda: (self.guarded,))
        if "SELECT id FROM events" in query:
            return Mock(fetchall=lambda: [(key,) for key in args[0]])
        return Mock(fetchall=lambda: self.rows)


@pytest.mark.parametrize("value", [None, [], {}, {**manifest(), "extra": 1},
    manifest({"bad/id": "a"*64}), manifest({"event": True}),
    manifest({"event": "a"*64}, {"event": "a"*64}),
    manifest({str(i): "a"*64 for i in range(21)})])
def test_invalid_manifest_never_connects(value):
    factory, switch = Mock(), Mock()
    with pytest.raises(ValueError, match="invalid_event_activation_manifest"):
        activation.authorize_and_activate(factory, value, switch)
    factory.assert_not_called()
    switch.assert_not_called()


def test_switch_is_inside_authorization_transaction(database):
    bundle, revision = approved_fixture(database)
    q = bundle["qualification"]
    conn = Connection([("event", revision)],
        [("event", revision, json.dumps(bundle), _version(q), json.dumps(q), None)])
    def switch():
        assert conn.open
        assert any("SHARE MODE NOWAIT" in sql for sql in conn.statements)
        assert any("FOR UPDATE NOWAIT" in sql for sql in conn.statements)
    activation.authorize_and_activate(lambda: conn, manifest({"event": revision}), switch)
    assert not conn.open


@pytest.mark.parametrize("fault", ["superseded", "missing", "new", "revoked", "guard", "bundle", "qualification", "autocommit", "transaction"])
def test_authorization_failures_never_switch(database, fault):
    bundle, revision = approved_fixture(database)
    q = copy.deepcopy(bundle["qualification"])
    pointers = [("event", revision)]
    revoked = None
    if fault == "superseded": pointers = [("event", "a"*64)]
    if fault == "missing": pointers = []
    if fault == "new": pointers += [("other", "a"*64)]
    if fault == "revoked": revoked = "revoked"
    if fault == "bundle": bundle["qualification"]["quotes"][0]["quote"] = "fabricated"
    if fault == "qualification": q["reviewer"]["id"] = "changed"
    conn = Connection(pointers, [("event", revision, json.dumps(bundle),
                                 _version(bundle["qualification"]), json.dumps(q), revoked)])
    if fault == "guard": conn.guarded = False
    if fault == "autocommit": conn.autocommit = True
    if fault == "transaction": conn.info = SimpleNamespace(transaction_status=psycopg.pq.TransactionStatus.INTRANS)
    switch = Mock()
    with pytest.raises(ValueError):
        activation.authorize_and_activate(lambda: conn, manifest({"event": revision}), switch)
    switch.assert_not_called()
    assert not conn.open


def test_explicit_withdrawal_keeps_inventory_without_reauthorizing():
    conn = Connection([("event", "a"*64)], [("event", "a"*64, "{}", "b"*64, "{}", "revoked")])
    switch = Mock()
    activation.authorize_and_activate(lambda: conn, manifest(withdrawn={"event": "a"*64}), switch)
    switch.assert_called_once_with()


def test_qualification_writer_cannot_act_as_activation_role():
    conn = Connection([], [])
    conn.writable = True
    switch = Mock()
    with pytest.raises(PermissionError, match="qualification_read_only_role_required"):
        activation.authorize_and_activate(lambda: conn, manifest(), switch)
    switch.assert_not_called()


@pytest.mark.parametrize("fault", ["missing", "symlink", "oversize", "directory", "fifo", "duplicate", "malformed"])
def test_manifest_files_fail_closed(tmp_path, fault):
    path = tmp_path / activation.MANIFEST
    if fault == "symlink": path.symlink_to(tmp_path / "other")
    if fault == "oversize": path.write_bytes(b" " * (activation.MAX_BYTES + 1))
    if fault == "directory": path.mkdir()
    if fault == "fifo": os.mkfifo(path)
    if fault == "duplicate": path.write_text('{"workflow":1,"workflow":2}')
    if fault == "malformed": path.write_text("{")
    with pytest.raises((OSError, ValueError)):
        activation.read_manifest(tmp_path)


def test_release_and_database_bounds_preserve_current(tmp_path, monkeypatch):
    releases = tmp_path / "releases"
    old, new = releases / "old", releases / "new"
    old.mkdir(parents=True)
    new.mkdir()
    (old / "index.html").write_text("old")
    (new / "index.html").write_text("new")
    (new / activation.MANIFEST).write_text(json.dumps(manifest()))
    current = tmp_path / "current"
    current.symlink_to("releases/old")
    monkeypatch.delenv("SV_EVENT_ACTIVATION_DB_URL", raising=False)
    with pytest.raises(ValueError, match="database_required"):
        activation.activate_release(new, current)
    assert current.readlink() == Path("releases/old")
    activation.activate_release(new, current, connection_factory=lambda: Connection([], []))
    assert current.readlink() == Path("releases/new")


@pytest.mark.parametrize("flag,guard_result,expected", [(None, 99, "new"), ("0", 99, "new"),
    ("1", 1, "old"), ("unexpected", 0, "old")])
def test_actual_shell_activation_branch_without_running_hugo(tmp_path, flag, guard_result, expected):
    script = (Path(__file__).parents[2] / "tools/hugo-build.sh").read_text()
    start = script.index('    case "${SV_EVENT_ACTIVATION_CHECK:-0}"')
    end = script.index("    esac", start) + len("    esac")
    branch = script[start:end]
    (tmp_path / "releases/old").mkdir(parents=True)
    (tmp_path / "releases/new").mkdir()
    current = tmp_path / "current"
    current.symlink_to("releases/old")
    env = {**os.environ, "CURRENT_LINK": str(current), "rel_release": "releases/new",
           "release_dir": str(tmp_path / "releases/new")}
    env.pop("SV_EVENT_ACTIVATION_CHECK", None)
    if flag is not None: env["SV_EVENT_ACTIVATION_CHECK"] = flag
    # Execute only the extracted switch branch, with no build executable involved.
    result = subprocess.run(["sh", "-c", f"python3() {{ return {guard_result}; }}\n" + branch],
                            env=env, capture_output=True)
    assert current.readlink() == Path("releases/" + expected)
    assert (result.returncode == 0) == (expected == "new")


def test_cli_failure_does_not_disclose_connection_details(monkeypatch, capsys):
    monkeypatch.setenv("SV_EVENT_ACTIVATION_CHECK", "1")
    monkeypatch.setattr(activation.sys, "argv", ["guard", "/release", "/current"])
    monkeypatch.setattr(activation, "activate_release", Mock(side_effect=RuntimeError("secret connection details")))
    assert activation.main() == 1
    output = capsys.readouterr().err
    assert "RuntimeError" in output and "secret" not in output
