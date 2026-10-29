-- Schedule ownership: 'manifest' rows were declared by a bundle's
-- shinyhub.toml and are disabled by a later full-manifest deploy that no
-- longer declares them; 'manual' rows (API, CLI, dashboard) are never touched
-- by a deploy. Existing rows are manual until a manifest declares them again.
ALTER TABLE app_schedules ADD COLUMN source TEXT NOT NULL DEFAULT 'manual';
