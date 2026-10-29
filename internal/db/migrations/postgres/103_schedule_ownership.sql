-- Existing rows remain manual until a deployment can establish ownership.
ALTER TABLE app_schedules ADD COLUMN managed_by TEXT NOT NULL DEFAULT 'manual' CHECK (managed_by IN ('manual', 'bundle'));
ALTER TABLE app_schedules ADD COLUMN retired INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1) AND (retired = 0 OR enabled = 0));
ALTER TABLE deployment_schedule_snapshots ADD COLUMN managed_by TEXT NOT NULL DEFAULT 'manual';
ALTER TABLE deployment_schedule_snapshots ADD COLUMN retired INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deployment_prior_schedule_snapshots ADD COLUMN managed_by TEXT NOT NULL DEFAULT 'manual';
ALTER TABLE deployment_prior_schedule_snapshots ADD COLUMN retired INTEGER NOT NULL DEFAULT 0;
