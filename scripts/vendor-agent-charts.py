#!/usr/bin/env python3
"""Reproduce isolated browser assets from exact `npm pack` tarballs (no network)."""

import argparse
import hashlib
import subprocess
import tarfile
from pathlib import Path

VERSIONS = {"vega": "6.4.0", "vega-lite": "6.5.0", "vega-embed": "7.3.0"}
PREFIX = b"/* ShinyHub namespace wrapper; upstream identifiers renamed deterministically. */\n(function (globalThis, exports, module, define) {\n"
SUFFIX = (
    b"\n}).call(window.shinyhubAgentChartVendor, window.shinyhubAgentChartVendor);\n"
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tarballs", type=Path)
    args = parser.parse_args()
    target = (
        Path(__file__).resolve().parents[1]
        / "packaging/python-agent/src/shinyhub_agent/www/vendor"
    )
    for name, version in VERSIONS.items():
        archive = args.tarballs / f"{name}-{version}.tgz"
        with tarfile.open(archive) as package:
            script = package.extractfile(f"package/build/{name}.min.js").read()
            license_text = package.extractfile("package/LICENSE").read()
        script = subprocess.run(
            ["node", str(Path(__file__).with_name("namespace-chart-vendor.mjs"))],
            input=script,
            stdout=subprocess.PIPE,
            check=True,
        ).stdout
        (target / f"{name}.min.js").write_bytes(PREFIX + script + SUFFIX)
        (target / f"{name}.LICENSE").write_bytes(license_text)
        print(
            f"{archive.name} sha256:{hashlib.sha256(archive.read_bytes()).hexdigest()}"
        )


if __name__ == "__main__":
    main()
