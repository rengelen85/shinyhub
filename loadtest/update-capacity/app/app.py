"""Reactive load fixture: CPU time and private, touched memory per session.

The driver sends actual Shiny init/update messages and checks the returned
sequence and session identity. Unlike idle socket tests, every acknowledged
input runs Python render work. process_time measures CPU, so throttling cannot
make the configured computation cheaper by advancing a wall-clock deadline.
"""
import json
import os
import time
import uuid
import cProfile
import asyncio
from pathlib import Path

from shiny import App, render, ui

VERSION = Path(__file__).with_name("version.txt").read_text().strip()
SESSION_KIB = int(os.environ.get("LOAD_SESSION_KIB", "128"))
DATA_FILE = Path(os.environ["SHINYHUB_APP_DATA"]) / "producer.txt"
DATA_SNAPSHOT = DATA_FILE.read_text() if DATA_FILE.exists() else "initial"

# cProfile changes the workload, so enable it only in a separate diagnostic
# pass. Start on a pressure-phase input, then stop/dump on the owning thread.
profiler = cProfile.Profile() if Path(__file__).with_name("profile.enabled").exists() else None
profile_started = False

app_ui = ui.page_fluid(ui.input_numeric("work_ms", "Computation (CPU milliseconds)", 50, min=1, max=100),
                       ui.input_checkbox("profile", "Profile worker (diagnostic only)", False),
                       ui.input_action_button("tick", "Compute"), ui.output_text_verbatim("result"))


def server(input, output, session):
    identity = uuid.uuid4().hex
    memory = bytearray(SESSION_KIB * 1024)
    for offset in range(0, len(memory), 4096):
        memory[offset] = 1

    @render.text
    def result():
        global profile_started
        if profiler is not None and input.profile() and not profile_started:
            profile_started = True
            profiler.enable()
            asyncio.get_running_loop().call_later(30, profiler.dump_stats, f"/tmp/load-worker-{os.getpid()}.pstats")
        started_ns = time.monotonic_ns()
        sequence = input.tick()
        cpu_ms = input.work_ms()
        end = time.process_time() + (cpu_ms / 1000 if sequence else 0)
        while time.process_time() < end:
            pass
        return json.dumps(dict(session=identity, tick=sequence, version=VERSION,
                               memory_bytes=len(memory), cpu_ms=cpu_ms, process_pid=os.getpid(),
                               data_snapshot=DATA_SNAPSHOT, render_started_ns=str(started_ns),
                               render_finished_ns=str(time.monotonic_ns())))


app = App(app_ui, server)
