"""Database integrity for lifetime autonomous admissions; no role/grant changes."""
SCHEMA = """
CREATE OR REPLACE FUNCTION event_report_v2_admission_guard() RETURNS trigger AS $$
BEGIN
 IF TG_OP='TRUNCATE' THEN
  IF EXISTS(SELECT 1 FROM event_source_report_runs WHERE snapshot_json::jsonb ? 'autonomous_policy') THEN
   RAISE EXCEPTION 'autonomous admission is immutable' USING ERRCODE='23514';
  END IF;
  RETURN NULL;
 END IF;
 IF OLD.snapshot_json::jsonb ? 'autonomous_policy' THEN
  IF TG_OP='DELETE' THEN
   RAISE EXCEPTION 'autonomous admission is immutable' USING ERRCODE='23514';
  END IF;
  IF ROW(NEW.run_id,NEW.event_id,NEW.request_key,NEW.trigger_kind,NEW.snapshot_json,
    NEW.source_version,NEW.generator_version,NEW.predecessor,NEW.budget_tokens,NEW.created_at,NEW.correction_enabled)
    IS DISTINCT FROM ROW(OLD.run_id,OLD.event_id,OLD.request_key,OLD.trigger_kind,OLD.snapshot_json,
    OLD.source_version,OLD.generator_version,OLD.predecessor,OLD.budget_tokens,OLD.created_at,OLD.correction_enabled) THEN
   RAISE EXCEPTION 'autonomous admission is immutable' USING ERRCODE='23514';
  END IF;
 ELSIF TG_OP='UPDATE' AND NEW.snapshot_json::jsonb ? 'autonomous_policy' THEN
  RAISE EXCEPTION 'autonomous provenance requires new admission' USING ERRCODE='23514';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS event_report_v2_admission_guard ON event_source_report_runs;
CREATE TRIGGER event_report_v2_admission_guard BEFORE UPDATE OR DELETE ON event_source_report_runs
 FOR EACH ROW EXECUTE FUNCTION event_report_v2_admission_guard();
DROP TRIGGER IF EXISTS event_report_v2_admission_truncate_guard ON event_source_report_runs;
CREATE TRIGGER event_report_v2_admission_truncate_guard BEFORE TRUNCATE ON event_source_report_runs
 FOR EACH STATEMENT EXECUTE FUNCTION event_report_v2_admission_guard();
"""
