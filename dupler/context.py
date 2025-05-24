from typing import Callable, TypeVar
import click


def get_instance() -> dict:
    ctx = click.get_current_context()
    ctx.ensure_object(dict)
    return ctx.obj


V = TypeVar("V")


def get_value(
    key: str, default: V | None = None, *, factory: Callable[[], V] | None = None
) -> V:
    ctx = get_instance()
    if key in ctx:
        return ctx[key]
    else:
        if factory is not None:
            default = factory()
            ctx[key] = default
        return default
