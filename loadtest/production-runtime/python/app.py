"""Dashboard fixture with loaded data, applied filters and private session state."""
import array
import json
import os
import time
import uuid
from pathlib import Path
from shiny import App, render, ui

VERSION = Path(__file__).with_name("version.txt").read_text().strip()
DATA = Path(os.environ["SHINYHUB_APP_DATA"])
if Path(__file__).with_name("fail-startup").exists():
    raise RuntimeError("deliberate candidate startup failure")
(DATA / f"starting-{VERSION}").write_text("starting")
while (DATA / f"gate-{VERSION}").exists():
    time.sleep(0.05)
time.sleep(1)
dataset = array.array("d")
with (DATA / "dataset.bin").open("rb") as source:
    dataset.fromfile(source, 2000000)
snapshot = (DATA / "producer.txt").read_text() if (DATA / "producer.txt").exists() else "initial"

app_ui = ui.page_fluid(ui.input_numeric("multiplier", "Applied filter", 2, min=1, max=10),
                       ui.input_action_button("tick", "Compute"), ui.output_text_verbatim("result"))


def server(input, output, session):
    identity = uuid.uuid4().hex
    private = bytearray(262144)
    for offset in range(0, len(private), 4096):
        private[offset] = 1

    @render.text
    def result():
        return json.dumps(dict(session=identity, tick=input.tick(), multiplier=input.multiplier(),
                               calculation=sum(dataset[:100000]) * input.multiplier(),
                               dataset_checksum=sum(dataset), dataset_bytes=len(dataset) * dataset.itemsize, memory_bytes=len(private),
                               version=VERSION, data_snapshot=snapshot, process_pid=os.getpid()))


app = App(app_ui, server)
