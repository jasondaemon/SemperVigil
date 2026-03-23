# Grafana Dashboards

These dashboards are optional observability assets for SemperVigil.

They are included with the project for operators who already run Grafana and compatible datasources, but Grafana is not a SemperVigil runtime dependency.

Current dashboards:
- `dashboards/sempervigil-queue-health.json`
- `dashboards/sempervigil-runner-control-plane.json`
- `dashboards/sempervigil-build-and-publish.json`
- `dashboards/sempervigil-live-logs.json`

Datasource expectations:
- `Queue Health`, `Runner Control Plane`, and `Build And Publish` expect Prometheus metrics from SemperVigil's `/metrics` endpoint.
- `Live Logs` expects a Loki datasource ingesting SemperVigil container stdout/stderr logs.

This directory is intentionally project-local so the dashboard definitions can version with SemperVigil changes without making Grafana mandatory for OSS users.
