-- Container lifecycle alarm failures. Keep visible independently of request errors.
SELECT COUNT(*) AS value
FROM logs.workersLogs
WHERE accountTag = 'd39e08a123830e48534a95a0116442dd'
  AND scriptName = 'shinyhub-demo'
  AND (level IN ('error', 'fatal') OR (error IS NOT NULL AND error != '') OR httpStatus >= 500)
  AND (httpStatus IS NULL OR httpStatus != 503 OR logType IS NULL OR logType != 'cf-worker-event')
  AND invocationType = 'alarm'
