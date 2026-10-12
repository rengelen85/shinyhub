"""Real bookmarked filter fixture for automatic version-switch acceptance."""
from pathlib import Path
import secrets

from shiny import App, render, ui
from shinyhub_bookmarks import Field, bookmarking_dependency, register

VERSION = Path(__file__).with_name("version.txt").read_text().strip()


def app_ui(request):
    return ui.page_fluid(
        bookmarking_dependency(),
        ui.input_numeric("samples", "Samples", value=1000, min=1, max=100000),
        ui.output_text_verbatim("result"),
    )


def server(input, output, session):
    identity = secrets.token_hex(4)
    register(session=session, input=input, fields={"samples": Field("Samples", baseline=1000)})

    @render.text
    def result():
        return f"Version: {VERSION}\nSession: {identity}\nSamples: {int(input.samples())}"


app = App(app_ui, server, bookmark_store="url")
