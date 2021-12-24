from . import utils


FIELD_BY_MODE = {
    "pid": {
        "default": "cyan",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "database": {
        "default": "black bold",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "application_name": {
        "default": "black bold",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "user": {
        "default": "black bold",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "client": {
        "default": "cyan",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "cpu": {
        "default": "normal",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "mem": {
        "default": "normal",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "read": {
        "default": "normal",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "write": {
        "default": "normal",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "time_red": {
        "default": "red",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "time_yellow": {
        "default": "yellow",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "time_green": {
        "default": "green",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "wait_green": {
        "default": "green bold",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "wait_red": {
        "default": "red bold",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "state_default": {
        "default": "normal",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "state_yellow": {
        "default": "yellow",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "state_green": {
        "default": "green",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "state_red": {
        "default": "red",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "query": {
        "default": "normal",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "relation": {
        "default": "cyan",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "type": {
        "default": "normal",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "mode_yellow": {
        "default": "yellow bold",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
    "mode_red": {
        "default": "red bold",
        "cursor": "cyan reverse",
        "yellow": "yellow bold",
    },
}


def short_state(state: str) -> str:
    state = utils.short_state(state)
    if state == "active":
        return "state_green"
    elif state == "idle in trans":
        return "state_yellow"
    elif state == "idle in trans (a)":
        return "state_red"
    else:
        return "state_default"


def lock_mode(mode: str) -> str:
    if mode in (
        "ExclusiveLock",
        "RowExclusiveLock",
        "AccessExclusiveLock",
    ):
        return "mode_red"
    else:
        return "mode_yellow"


def wait(value: bool) -> str:
    return "wait_red" if value else "wait_green"
