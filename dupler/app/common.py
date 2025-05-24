from rich.text import Text
from textual import on
from textual.app import ComposeResult
from textual.containers import Grid
from textual.events import DescendantFocus
from textual.screen import Screen
from textual.widgets import Button, Label, RichLog, Static


from typing import Callable


class Ask(Screen):
    CSS = """
    Ask { align: center middle; padding: 1 1;}
    #dialog {
        grid-size: 2;
        grid-gutter: 1 2;
        grid-rows: 1fr 3;
        width: 50;
        height: 16;
        border: solid yellow;
        background: $surface;
    }
    #question {
        column-span: 2;
        width: 1fr;
        height: 1fr;
        content-align: center middle;
    }
    Button {
        width: 100%;
    }
    """

    def __init__(self, question: str, action: Callable):
        super().__init__()
        self.question = question
        self.action = action

    def compose(self) -> ComposeResult:
        with Grid(id="dialog"):
            yield Label(self.question, id="question")
            yield Button("Yes", variant="success", id="yes")
            yield Button("No", variant="error", id="no")

    def on_mount(self, event: DescendantFocus) -> None:
        self.query_one("#yes").focus()

    @on(Button.Pressed)
    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.app.pop_screen()
        if event.button.id == "yes":
            self.action()


class LogScreen(Screen):
    BINDINGS = [
        ("escape", "app.pop_screen", "Pop screen"),
    ]

    def __init__(self):
        super().__init__()
        self.__log = RichLog()
        self.__log.write(Text("Log STARTED", style="bold"))

    def compose(self) -> ComposeResult:
        yield Static("Log Widow", id="title")
        yield self.__log

    def write(self, *args, **kwargs):
        self.__log.write(*args, **kwargs)

    def on_mount(self, event: DescendantFocus) -> None:
        self.__log.focus()
