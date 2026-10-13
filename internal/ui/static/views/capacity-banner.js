// A blocked deployment leaves the serving app healthy. Explain the manual
// retry separately from crash/connectivity warnings and automatic roll retries.
export function capacityBanner(doc, envelope) {
  const update = envelope?.update_status;
  if (update?.state !== 'blocked_capacity') return null;
  const banner = doc.createElement('div');
  banner.className = 'conn-banner capacity-banner';
  banner.setAttribute('role', 'status');
  const title = doc.createElement('div');
  title.className = 'conn-banner-title';
  title.textContent = 'Update blocked by capacity';
  const body = doc.createElement('p');
  body.className = 'conn-banner-body';
  body.textContent = `${update.reason}. Free or add capacity, then retry the deployment. This deployment will not retry automatically.`;
  banner.append(title, body);
  return banner;
}
