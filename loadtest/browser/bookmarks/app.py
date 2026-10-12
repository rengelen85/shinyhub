"""Real bookmarked filter fixture for automatic version-switch acceptance."""

from pathlib import Path
import secrets

from shiny import App, reactive, render, ui
from shinyhub_bookmarks import ChoiceRestore, Field, bookmarking_dependency, register

VERSION = Path(__file__).with_name("version.txt").read_text().strip()


def app_ui(request):
    return ui.page_fluid(
        bookmarking_dependency(),
        ui.input_numeric("samples", "Samples", value=1000, min=1, max=100000),
        ui.input_select(
            "region", "Region", choices=["EU", "US", "APAC"], selected="EU"
        ),
        ui.input_action_button("apply", "Apply"),
        ui.output_text_verbatim("result"),
    )


def server(input, output, session):
    identity = secrets.token_hex(4)
    register(
        session=session,
        input=input,
        commit=input.apply,
        fields={
            "samples": Field("Samples", baseline=1000),
            "region": Field(
                "Region",
                baseline="EU",
                restore=ChoiceRestore(choices=["EU", "US", "APAC"], default="EU"),
            ),
        },
    )

    @reactive.calc
    @reactive.event(input.apply, ignore_none=False)
    def applied():
        region = input.region()
        return int(input.samples()), region if region in ["EU", "US", "APAC"] else "EU"

    @render.text
    def result():
        samples, region = applied()
        return f"Version: {VERSION}\nSession: {identity}\nSamples: {samples}\nRegion: {region}"


app = App(app_ui, server, bookmark_store="url")
