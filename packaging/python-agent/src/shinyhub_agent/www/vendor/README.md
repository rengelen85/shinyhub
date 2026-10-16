# Chart renderer dependencies

These browser distributions are vendored from the corresponding npm packages:

| Package | Version | File | License |
| --- | --- | --- | --- |
| vega | 6.4.0 | build/vega.min.js | BSD-3-Clause (`vega.LICENSE`) |
| vega-lite | 6.5.0 | build/vega-lite.min.js | BSD-3-Clause (`vega-lite.LICENSE`) |
| vega-embed | 7.3.0 | build/vega-embed.min.js | BSD-3-Clause (`vega-embed.LICENSE`) |

Upstream: https://github.com/vega/vega, https://github.com/vega/vega-lite,
https://github.com/vega/vega-embed. Keep each license beside its distribution.

To update, download exact packages with `npm pack <package>@<version>` into a
temporary directory, then run `python3 scripts/vendor-agent-charts.py <directory>`
from the repository root after `npm ci`, and update this table. Pinned Terser
renames local identifiers deterministically to neutral names, without optimizing
the code or mangling properties. The script then wraps each distribution in a
private namespace and shadows AMD/CommonJS hooks,
so host app globals and module loaders remain unaffected. Run the Python agent suite, JavaScript chat tests, and
`scripts/agent-charts-browser-e2e.mjs` against the new distributions. The latter
needs Playwright, Chrome, and axe-core (the repository's browser-test tooling).

The libraries load on demand from the app's own helper dependency directory.
`charts.js` constructs every spec and disables external loading and editor links.

Source tarball SHA-256 checksums:

```text
3521fb6ff68a94a40323954f0ba4484ba4014b157bfd37f89a3b063082320c42  vega-6.4.0.tgz
811230a6bfd2cdbfc359d0c38f624a90df86657b7f251b67be88bcc4eee32b7f  vega-lite-6.5.0.tgz
d9a958ced3240f0418df7d5e12e97dba6e63cecda15b8403e47a0db7eb6ea859  vega-embed-7.3.0.tgz
```
