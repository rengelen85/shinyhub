-- Remember only lifecycle intent displaced by schedule data uncertainty.
ALTER TABLE apps ADD COLUMN producer_recovery_status TEXT NOT NULL DEFAULT '';
