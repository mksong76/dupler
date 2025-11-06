from typing import Callable

from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, HorizontalGroup
from textual.css.query import NoMatches
from textual.events import DescendantFocus, Message
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


class UpdateItems(Message):
    def __init__(self, cause: str):
        super().__init__()
        self.cause = cause

class DeDeplicate(App):
    BINDINGS = [
        ("q", "quit", "Quit the application"),
        ("n", "next_new", "Next item to select"),
        ("p", "prev_new", "Previous item to select"),
        ("right", "next", "Next item to select"),
        ("left", "prev", "Previous item to select"),
        ("a", "apply", "Apply changes"),
        ("l", "app.push_screen('log')", "Show log"),
        ("r", "refresh", "Refresh items"),
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

        self.d_start = 0
        self.d_end = 0

    def on_mount(self) -> None:
        self.install_screen(self.logs, name="log")
        lv = self.query_one("#list", ListView)
        lv.animate = False
        self.animate = False
        self.post_message(UpdateItems('mount'))

    async def action_refresh(self):
        self.post_message(UpdateItems('refresh'))

    @on(UpdateItems)
    async def on_update_items(self, message: UpdateItems) -> None:
        if await self.update_items(message.cause):
            self.post_message(UpdateItems('recheck'))

    @staticmethod
    def item_height(item: ListItem) -> int:
        return item.content_size.height+item.gutter.top+item.gutter.bottom
    @staticmethod
    def data_margin(lv: ListView) -> tuple[int, int]:
        on_screen_start = None
        on_screen_end = None
        for idx, item in enumerate(lv.children):
            if on_screen_start is None:
                if lv.can_view_partial(item):
                    on_screen_start = idx
            elif on_screen_end is None:
                if not lv.can_view_partial(item):
                    on_screen_end = idx
                    break
        if on_screen_start is not None:
            on_screen_end = on_screen_end or len(lv.children)
            return on_screen_start, len(lv.children) - on_screen_end
        else:
            return 0, len(lv.children)

    async def update_items(self, cause: str):
        lv = self.query_one("#list", ListView)
        # lv.scroll_to(immediate=True, animate=False)
        top_margin, bottom_margin = self.data_margin(lv)

        self.logs.write(f"UPDATE[{cause}] tm={top_margin} bm={bottom_margin} children={len(lv.children)}")
        self.logs.write(f"    offset={lv.scroll_offset} sy={lv.scroll_y} sty={lv.scroll_target_y}")

        current_idx = lv.index or 0
        top_changed = False
        bottom_changed = False

        if top_margin > 3 and self.d_start < self.d_end:
            to_remove = min(top_margin - 3, self.d_end - self.d_start)
            content_height = sum(tuple(
                self.item_height(x) for x in lv.children[:to_remove]
            ))
            async with lv.batch():
                await lv.remove_items(range(0, to_remove))
                scroll_y = lv.scroll_target_y-content_height
                self.logs.write(f'    REMOVE {to_remove} items scroll_y={lv.scroll_y} <- {scroll_y}')
                lv.scroll_to(y=scroll_y, immediate=True, animate=False)
                lv.index = current_idx-to_remove
            self.d_start += to_remove
            top_margin -= to_remove
            top_changed = True
        if bottom_margin > 3 and self.d_end > self.d_start:
            to_remove = min(bottom_margin-3, self.d_end-self.d_start)
            length = len(lv.children)
            await lv.remove_items(list(range(length-to_remove, length)))
            self.d_end -= to_remove
            bottom_margin -= to_remove
            bottom_changed = True
        if top_margin < 3 and self.d_start > 0:
            to_insert = min(3-top_margin, self.d_start)
            items = [
                self.build_list_item(x, self.duplicates[self.keys[x]])
                for x in range(self.d_start - to_insert, self.d_start)
            ]
            async with lv.batch():
                content_height = self.item_height(lv.children[0])*to_insert
                await lv.insert(0, items)
                self.logs.write(f'    INSERT {to_insert} {lv.scroll_y=} {content_height=}')
                lv.scroll_to(y=lv.scroll_y+content_height, immediate=True, animate=False)
                lv.index = current_idx+to_insert
            self.d_start -= to_insert
            top_margin += to_insert
            top_changed = True
        if bottom_margin < 3 and self.d_end < len(self.keys):
            to_append = min(3-bottom_margin, len(self.keys)-self.d_end)
            items = [
                self.build_list_item(x, self.duplicates[self.keys[x]])
                for x in range(self.d_end, self.d_end + to_append)
            ]
            await lv.extend(items)
            self.d_end += to_append
            bottom_margin += to_append
            bottom_changed = True
        # if top_changed:
        #     lv.index = current_idx
        self.logs.write(f"UPDATE[{cause}] end sty={lv.scroll_target_y} {top_changed} {bottom_changed}")
        return top_changed or bottom_changed

    @on(ListView.Highlighted)
    def on_listview_highlighted(self, event: ListView.Highlighted):
        self.post_message(UpdateItems(f"highlighted({event.list_view.children.index(event.item)})"))

    def build_list_item(self, sn: int, duplicate: Duplicate) -> ListItem:
        value = self.selection.get(duplicate.hash, Select.BLANK)
        return ListItem(
            Horizontal(
                Digits(f"{sn+1}", classes="sn"),
                Container(
                    Static(
                        f"{duplicate.name} / {duplicate.size:,d} bytes",
                        markup=False,
                        classes="hash",
                    ),
                    Select(
                        self.options_for_files(duplicate.files),
                        prompt="Select a file to keep",
                        id=f"file-{duplicate.hash.hex()}",
                        compact=False,
                        value=value,
                    ),
                ),
            ),
            id=f"item-{duplicate.hash.hex()}",
            classes="list-item",
        )

    def compose(self) -> ComposeResult:
        yield Header()
        yield ListView(id="list")
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
                    if hash in self.selection:
                        continue
                    value = [x for x in duplicate.files if x.path_id == dir_id][
                        0
                    ].id
                    self.set_selection(hash, value)
                    try:
                        s = self.query_one(f"#file-{hash.hex()}", Select)
                    except NoMatches:
                        continue
                    with s.prevent(Select.Changed):
                        s.value = value
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
        if isinstance(value, int) and value is not Select.BLANK:
            if self.selection.get(hash) == value:
                return
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
        return sum(file.object.size for file in self.duplicates[hash].files if file.id != id)

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
