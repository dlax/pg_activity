import functools
import itertools
from typing import Any, Iterable, Iterator, List, Optional, Tuple

from rich.columns import Columns
from rich.console import NewLine, RenderableType, group
from rich.style import Style
from rich.table import Column as TableColumn, Table
from rich.text import Text

from .keys import (
    BINDINGS,
    EXIT_KEY,
    HELP as HELP_KEY,
    KEYS_BY_QUERYMODE,
    Key,
    MODES,
    PAUSE_KEY,
    PROCESS_CANCEL,
    PROCESS_KILL,
    PROCESS_PIN,
)
from .types import (
    Host,
    IOCounter,
    Pct,
    SelectableProcesses,
    ServerInformation,
    SystemInfo,
    UI,
)
from . import colors, utils


@group()
def help(version: str, is_local: bool) -> Iterator[RenderableType]:
    """Render help menu."""
    project_url = "https://github.com/dalibo/pg_activity"

    def key_mappings(keys: Iterable[Key]) -> Iterator[Text]:
        for key in keys:
            key_name = key.name or key.value
            yield Text.assemble(
                (key_name.rjust(10), "bold cyan"), ": ", key.description
            )

    yield Text.assemble((f"pg_activity {version}", "bold green"), f" - {project_url}")
    yield Text("Released under PostgreSQL License.")

    yield NewLine()

    bindings = BINDINGS
    if not is_local:
        bindings = [b for b in bindings if not b.local_only]
    yield from key_mappings(bindings)

    yield Text("Mode")
    yield from key_mappings(MODES)

    yield NewLine()

    yield Text("Press any key to exit.")


@group()
def header(
    ui: UI,
    *,
    host: Host,
    pg_version: str,
    server_information: ServerInformation,
    system_info: Optional[SystemInfo] = None,
) -> Iterator[RenderableType]:
    """Return window header lines."""

    @functools.singledispatch
    def render(x: Any) -> str:
        if x is None:
            return "-"
        raise AssertionError(f"not implemented for type '{type(x).__name__}'")

    @render.register(str)
    def render_str(s: str) -> str:
        return f"[bold green]{s}[/bold green]"

    @render.register(int)
    def render_int(n: int) -> str:
        return render(str(n))

    @render.register(Pct)
    def render_pct(n: Pct) -> str:
        return render(f"{n:.2f}%")

    @render.register(float)
    def render_float(n: float) -> str:
        return render(f"{n:.2f}")

    @render.register(IOCounter)
    def render_iocounter(i: IOCounter) -> str:
        hbytes = utils.naturalsize(i.bytes) + "/s"
        counts = str(i.count) + "/s"
        return f"{render(hbytes)} - {render(counts)}"

    def columns(*items: str) -> Columns:
        return Columns(map(Text.from_markup, items), padding=(0, 2))

    def ratio(n: Any, d: Any) -> str:
        return f"{render(n)}/{render(d)}"

    pg_host = f"{host.user}@{host.host}:{host.port}/{host.dbname}"
    dash = " - "
    h1 = Text.assemble(
        pg_version,
        dash,
        (host.hostname, "bold"),
        dash,
        (pg_host, "cyan"),
        dash,
        f"Ref.: {ui.refresh_time}s",
        dash,
        f"Duration mode: {ui.duration_mode.name}",
        overflow="crop",
        no_wrap=True,
    )
    if ui.min_duration:
        h1.append(dash)
        h1.append(f"Min. duration: {ui.min_duration}s")
    yield h1

    si = server_information

    total_size = utils.naturalsize(si.total_size)
    size_ev = f"{utils.naturalsize(si.size_evolution)}/s"
    uptime = utils.naturaltimedelta(si.uptime)

    if ui.show_instance_info_in_header:
        # First rows are always displayed, as the underlying data is always available.
        yield columns(
            f"* Global: {render(uptime)} uptime",
            f"{render(total_size)} dbs size - {render(size_ev)} growth",
            f"{render(si.cache_hit_ratio)} cache hit ratio",
        )

        yield columns(
            f"  Sessions: {ratio(si.total, si.max_connections)} total",
            f"{render(si.active_connections)} active",
            f"{render(si.idle)} idle",
            f"{render(si.idle_in_transaction)} idle in txn",
            f"{render(si.idle_in_transaction_aborted)} idle in txn abrt",
            f"{render(si.waiting)} waiting",
        )

        temp_size = utils.naturalsize(si.temp_bytes)
        yield columns(
            f"  Activity: {render(si.tps)} tps",
            f"{render(si.insert_per_second)} insert/s",
            f"{render(si.update_per_second)} update/s",
            f"{render(si.delete_per_second)} delete/s",
            f"{render(si.tuples_returned_per_second)} tuples returned/s",
            f"{render(si.temp_files)} temp files",
            f"{render(temp_size)} temp size",
        )
    if ui.show_worker_info_in_header:
        yield columns(
            f"* Worker processes: {ratio(si.worker_processes, si.max_worker_processes)} total",
            f"{ratio(si.logical_replication_workers, si.max_logical_replication_workers)} logical workers",
            f"{ratio(si.parallel_workers, si.max_parallel_workers)} parallel workers",
        )

        yield columns(
            f"  Other processes & info: {ratio(si.autovacuum_workers, si.autovacuum_max_workers)} autovacuum workers",
            f"{ratio(si.wal_senders, si.max_wal_senders)} wal senders",
            f"{render(si.wal_receivers)} wal receivers",
            f"{ratio(si.replication_slots, si.max_replication_slots)} repl. slots",
        )

    # System information, only available in "local" mode.
    if system_info is not None and ui.show_system_info_in_header:
        used, bc, free, total = (
            utils.naturalsize(system_info.memory.used),
            utils.naturalsize(system_info.memory.buff_cached),
            utils.naturalsize(system_info.memory.free),
            utils.naturalsize(system_info.memory.total),
        )
        yield columns(
            f"* Mem.: {render(total)} total",
            f"{render(free)} ({render(system_info.memory.pct_free)}) free",
            f"{render(used)} ({render(system_info.memory.pct_used)}) used",
            f"{render(bc)} ({render(system_info.memory.pct_bc)}) buff+cached",
        )

        used, free, total = (
            utils.naturalsize(system_info.swap.used),
            utils.naturalsize(system_info.swap.free),
            utils.naturalsize(system_info.swap.total),
        )
        yield columns(
            f"  Swap: {render(total)} total",
            f"{render(free)} ({render(system_info.swap.pct_free)}) free",
            f"{render(used)} ({render(system_info.swap.pct_used)}) used",
        )

        iops = f"{system_info.max_iops}/s"
        yield columns(
            f"  IO: {render(iops)} max iops",
            f"{render(system_info.io_read)} read",
            f"{render(system_info.io_write)} write",
        )

        load = system_info.load
        yield columns(
            f"  Load average: {render(load.avg1)} {render(load.avg5)} {render(load.avg15)}"
        )


def processes(ui: UI, processes: SelectableProcesses, maxlines: int) -> Table:
    """Return the of table processes."""
    if ui.in_pause:
        title = Text("PAUSE", style=Style(color="black", bgcolor="yellow"))
    else:
        title = Text(ui.query_mode.value.upper(), style="bold green")
    table = Table(
        title=title,
        title_justify="center",
        box=None,
        padding=(0, 1, 0, 0),
        expand=True,
    )
    for column in ui.columns():
        overflow = "crop"
        if column.key == "query" and not ui.wrap_query:
            overflow = "ignore"
        table.add_column(
            column.name,
            header_style=Style(
                color=column.title_color(ui.sort_key), reverse=True, bold=False
            ),
            justify=column.justify,  # type: ignore[arg-type]
            overflow=overflow,  # type: ignore[arg-type]
        )

    maxlines -= 2  # title line, column headers line

    position = processes.position()
    if position is None:
        display_processes = iter(processes)
    else:
        # Scrolling is handled here. We just have to manage the start position of the
        # display. the length is managed by @limit.
        if ui.wrap_query:
            # When the query is wrapped, we always display selected process first (sort
            # of relative scrolling).
            start = position
        else:
            # Otherwise, we compute the start position of the table and try to have a 5 lines
            # leeway at the bottom of the page to increase readability.
            start = 0
            bottom = int(maxlines // 5)
            if position is not None and position >= maxlines - bottom:
                start = position - maxlines + 1 + bottom

        display_processes = itertools.chain(
            iter(processes[-(len(processes) - start) :]), iter(processes[:start])
        )

    focused, pinned = processes.focused, processes.pinned

    for process in display_processes:

        if process.pid == focused:
            color_type = "cursor"
            style = "reverse cyan"
        elif process.pid in pinned:
            color_type = "yellow"
            style = "bold yellow"
        else:
            color_type = "default"
            style = None

        row = []
        for column in ui.columns():
            value = getattr(process, column.key)
            row.append(
                Text(
                    column.render(value, process),
                    style=colors.FIELD_BY_MODE[column.color(value)][color_type],
                )
            )
        table.add_row(*row, style=style)

    return table


def footer_help() -> Table:
    """Footer line with help keys."""
    query_modes_help = [
        ("/".join(keys[:-1]), qm.value) for qm, keys in KEYS_BY_QUERYMODE.items()
    ]
    assert PAUSE_KEY.name is not None
    footer_values = query_modes_help + [
        (PAUSE_KEY.name, PAUSE_KEY.description),
        (EXIT_KEY.value, EXIT_KEY.description),
        (HELP_KEY, "help"),
    ]
    return render_footer(footer_values)


def render_footer(footer_values: List[Tuple[str, str]]) -> Table:
    headers, row = [], []
    for key, desc in footer_values:
        headers.append(TableColumn(justify="center", overflow="ignore"))
        row.append(Text.assemble(key, " ", (desc.capitalize(), "cyan reverse")))
    table = Table.grid(*headers, padding=(0, 1))
    table.add_row(*row)
    return table


def footer_interative_help() -> Table:
    """Footer line with help keys for interactive mode."""
    assert PROCESS_PIN.name is not None
    footer_values = [
        (PROCESS_CANCEL, "cancel current query"),
        (PROCESS_KILL, "terminate current query"),
        (PROCESS_PIN.name, PROCESS_PIN.description),
        ("Other", "back to activities"),
        (EXIT_KEY.value, EXIT_KEY.description),
    ]
    return render_footer(footer_values)


def footer(ui: UI, *, message: Optional[Tuple[str, str]] = None) -> RenderableType:
    if message is not None:
        table = Table.grid(expand=True)
        table.add_column(justify="center", overflow="ignore")
        msg, color = message
        table.add_row(msg, style=color)
        return table
    elif ui.interactive():
        return footer_interative_help()
    else:
        return footer_help()
