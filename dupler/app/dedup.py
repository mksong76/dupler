from typing import Callable, Optional

from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, HorizontalGroup
from textual.events import DescendantFocus
from textual.widgets import (
    Button,
    Digits,
    Header,
    Label,
    ListItem,
    ListView,
    Select,
    Static,
)

from .. import model
from ..filemanager import FileManager, Duplicate
from .common import Ask, LogScreen


class DeDeplicate(App):
    BINDINGS = [
        ("q", "quit", "Quit the application"),
        ("n", "next_new", "Next item to select"),
        ("p", "prev_new", "Previous item to select"),
        ("right", "next", "Next item to select"),
        ("left", "prev", "Previous item to select"),
        ("a", "apply", "Apply changes"),
        ("l", "app.push_screen('log')", "Show log"),
    ]
    CSS_PATH = "dedup.tcss"

    def __init__(self, fm: FileManager, duplicates: dict[bytes, Duplicate]):
        super().__init__()

        self.title = "De-Duplicate"

        self.fm = fm
        self.duplicates = duplicates
        self.keys = [k for k in duplicates.keys()]
        self.selection: dict[bytes, int] = {}
        self.reduced: dict[bytes, int] = {}
        self.apply = False
        self.logs = LogScreen()

    def on_mount(self) -> None:
        self.install_screen(self.logs, name="log")

    def compose(self) -> ComposeResult:
        yield Header()
        with ListView(id="list"):
            cnt = 0
            for hash, duplicate in self.duplicates.items():
                yield ListItem(
                    HorizontalGroup(
                        Digits(f"{cnt+1}", classes="sn"),
                        Container(
                            Static(
                                f"{duplicate.name} / {duplicate.size:,d} bytes",
                                markup=False,
                                classes="hash",
                            ),
                            Select(
                                self.options_for_files(duplicate.files),
                                prompt="Select a file to keep",
                                id=f"file-{hash.hex()}",
                            ),
                        ),
                    ),
                    id=f"item-{hash.hex()}",
                    classes="list-item",
                )
                cnt += 1
        yield Horizontal(
            Label("Reducing Size\n(BYTES)", classes="label"),
            Digits("0", id="reduced", classes="bytes"),
            Label("Processed\n(PROCESSED/TOTAL)", classes="label"),
            Digits(f"0 / {len(self.duplicates)}", id="processed", classes="count"),
            Button("Apply(a)", id="apply"),
            classes="bottom",
        )

    @on(ListView.Selected)
    def on_select_item(self, event: ListView.Selected) -> None:
        event.item.query_one(Select).action_show_overlay()

    def ask(self, question: str, action: Callable):
        dialog = Ask(question, action)
        self.push_screen(dialog)

    def set_selection(self, hash: bytes, value: int):
        self.selection[hash] = value
        self.reduced[hash] = self.calculate_reduced(hash)

    def action_next_new(self):
        lv = self.query_one("#list", ListView)
        index = lv.index
        if index is None:
            lv.index = 0
            return

        for i in range(index + 1, len(self.keys)):
            hash = self.keys[i]
            if hash not in self.selection:
                lv.index = i
                return

    def action_prev_new(self):
        lv = self.query_one("#list", ListView)
        index = lv.index
        if index is None:
            lv.index = 0
            return
        for i in range(index - 1, 0, -1):
            hash = self.keys[i]
            if hash not in self.selection:
                lv.index = i
                return

    def apply_for_directory_prefers(
        self, origin: bytes, dir_id: int, dirset: set[int]
    ) -> Callable:
        def apply():
            for hash, duplicate in self.duplicates.items():
                if hash == origin:
                    continue
                dirset2 = {x.path_id for x in duplicate.files}
                if dirset == dirset2:
                    s = self.query_one(f"#file-{hash.hex()}", Select)
                    if s.value == Select.BLANK:
                        value = [x for x in duplicate.files if x.path_id == dir_id][0].id
                        with s.prevent(Select.Changed):
                            s.value = value
                        self.set_selection(hash, value)
            self.update_statics()

        return apply

    def try_generic(self, hash: bytes, value: int):
        duplicate = self.duplicates[hash]
        names = [file.name for file in duplicate.files]
        name = names.pop(0)
        if all([name == x for x in names]):
            prefer = [file.directory for file in duplicate.files if file.id == value][0]
            dir_set = {file.path_id for file in duplicate.files}
            self.ask(
                f"Apply preference to {prefer.path} ",
                self.apply_for_directory_prefers(hash, prefer.id, dir_set),
            )

    @on(Select.Changed)
    def on_select_changed(self, event: Select.Changed) -> None:
        id_str = event.select.id or ""
        if not id_str.startswith("file-"):
            return
        hash = bytes.fromhex(id_str[len("file-") :])
        value = event.select.value
        if isinstance(value, int) and value != Select.BLANK:
            self.set_selection(hash, value)
            self.try_generic(hash, value)
        else:
            del self.selection[hash]
            del self.reduced[hash]
        self.update_statics()

    @staticmethod
    def options_for_files(files: list[model.File]):
        return [
            (
                Text(f.get_path()),
                f.id,
            )
            for f in files
        ]

    def calculate_reduced(self, hash) -> int:
        id = self.selection[hash]
        return sum(file.object.size for file in self.duplicates[hash] if file.id != id)

    def update_statics(self):
        reduced = sum(self.reduced.values())
        self.query_one("#reduced", Digits).update(f"{reduced:,d}")
        self.query_one("#processed", Digits).update(
            f"{len(self.selection):,d} / {len(self.duplicates):,d}"
        )

    async def action_apply(self):
        self.apply = True
        # self.do_apply()
        await self.action_quit()

    def do_apply(self):
        to_remove: list[bytes] = []
        to_update: list[bytes] = []
        errors: list[str] = []
        for hash, id in self.selection.items():
            removed: list[int] = []
            for file in self.duplicates[hash]:
                if file.id != id:
                    try:
                        self.fm.delete_file(file)
                        removed.append(file.id)
                    except Exception as e:
                        errors.append(str(e))

            if len(removed) == 0:
                continue

            new_files = [f for f in self.duplicates[hash] if f.id not in removed]
            if len(new_files) == 1:
                self.duplicates.pop(hash)
                to_remove.append(hash)
            else:
                self.duplicates[hash] = new_files
                self.reduced[hash] = self.calculate_reduced(hash)

        indexes = []
        for hash in to_remove:
            indexes.append(self.keys.index(hash))
            self.selection.pop(hash)

        view = self.query_one("#list", ListView)
        view.remove_items(indexes)
        self.keys = [k for k in self.duplicates.keys()]

        for hash in to_update:
            selector = self.query_one("#file-" + hash.hex(), Select)
            selector.set_options(self.options_for_files(self.duplicates[hash].files))

        self.update_statics()

    @on(DescendantFocus)
    def on_descendant_focus(self, event: DescendantFocus) -> None:
        if isinstance(event.widget, Select):
            lst = self.query_one("#list", ListView)
            lst.focus()
