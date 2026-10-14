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
from pathlib import Path

from shiny import App, render, ui

VERSION = Path(__file__).with_name("version.txt").read_text().strip()
SESSION_KIB = int(os.environ.get("LOAD_SESSION_KIB", "128"))
DATA_FILE = Path(os.environ["SHINYHUB_APP_DATA"]) / "producer.txt"
DATA_SNAPSHOT = DATA_FILE.read_text() if DATA_FILE.exists() else "initial"

app_ui = ui.page_fluid(ui.input_numeric("work_ms", "Computation (CPU milliseconds)", 50, min=1, max=100),
                       ui.input_action_button("tick", "Compute"), ui.output_text_verbatim("result"))


def server(input, output, session):
    identity = uuid.uuid4().hex
    memory = bytearray(SESSION_KIB * 1024)
    for offset in range(0, len(memory), 4096):
        memory[offset] = 1

    @render.text
    def result():
        sequence = input.tick()
        cpu_ms = input.work_ms()
        end = time.process_time() + (cpu_ms / 1000 if sequence else 0)
        while time.process_time() < end:
            pass
        return json.dumps(dict(session=identity, tick=sequence, version=VERSION,
                               memory_bytes=len(memory), cpu_ms=cpu_ms, process_pid=os.getpid(),
                               data_snapshot=DATA_SNAPSHOT))


app = App(app_ui, server)
