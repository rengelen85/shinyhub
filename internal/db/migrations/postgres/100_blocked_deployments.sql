-- Admission refusals preserve the serving version. Repair historical badges
-- without reclassifying failures that crossed a producer compatibility barrier.
UPDATE deployments
SET status = 'blocked'
WHERE status = 'failed'
  AND failure_reason LIKE 'capacity-blocked/%'
  AND producer_barrier_entered = 0;
