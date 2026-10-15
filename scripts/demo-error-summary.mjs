// Offline companion to the alert: the adaptive SQL engine cannot count DISTINCT.
// Input must already be filtered to the relevant Worker and error records.
export function summarizeErrors(records) {
  const categories = { request: { records: 0, groups: new Set() }, lifecycle: { records: 0, groups: new Set() } };
  records.forEach((record, index) => {
    const metadata = record.$metadata ?? {};
    const workers = record.$workers ?? {};
    const kind = record.invocationType ?? workers.eventType ?? metadata.origin;
    const category = categories[kind === "alarm" ? "lifecycle" : "request"];
    const first = (...values) => values.find(value => typeof value === "string" && value.length > 0);
    const trace = first(record.traceId, metadata.traceId);
    const request = first(record.requestId, metadata.requestId);
    const log = first(record.id, metadata.id);
    // Prefixes prevent accidental collisions between different ID namespaces.
    const key = trace ? `trace:${trace}` : request ? `request:${request}` : log ? `log:${log}` : `row:${index}`;
    category.records++;
    category.groups.add(key);
  });
  return Object.fromEntries(Object.entries(categories).map(([name, value]) => [name, {
    error_records: value.records, correlated_failure_groups: value.groups.size,
  }]));
}

import { pathToFileURL } from "node:url";
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    let input = "";
    for await (const chunk of process.stdin) input += chunk;
    const text = input.trim();
    const records = text.startsWith("[") ? JSON.parse(text) : text.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
    if (!Array.isArray(records) || records.some(row => !row || typeof row !== "object" || Array.isArray(row))) {
      throw new Error("Expected exported records as a JSON array or JSONL");
    }
    console.log(JSON.stringify(summarizeErrors(records), null, 2));
  } catch {
    console.error("Could not read error records. Supply a JSON array or JSONL of matching Worker error records.");
    process.exitCode = 1;
  }
}
