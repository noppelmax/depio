"""TUI rendering for the Pipeline live display.

All functions receive the Pipeline object and return Rich renderables.
They are private to the depio package (_-prefixed module).
"""

import shutil
from typing import TYPE_CHECKING, Dict

from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from .TaskStatus import TaskStatus

if TYPE_CHECKING:
    from .Pipeline import Pipeline


_STATUS_DISPLAY = {
    TaskStatus.PENDING:   ("·",  "dim"),
    TaskStatus.WAITING:   ("◉",  "blue"),
    TaskStatus.RUNNING:   ("●",  "bold yellow"),
    TaskStatus.FINISHED:  ("✓",  "bold green"),
    TaskStatus.SKIPPED:   ("✓",  "green"),
    TaskStatus.FAILED:    ("✗",  "bold red"),
    TaskStatus.DEPFAILED: ("✗",  "red"),
    TaskStatus.CANCELED:  ("⊘",  "dim"),
    TaskStatus.HOLD:      ("⏸", "dim"),
    TaskStatus.UNKNOWN:   ("?",  "dim"),
}


def render_task_list(p: "Pipeline") -> Panel:
    tui = p.tui
    has_slurm = any(task.slurmjob is not None for task in p.tasks)

    display_list = [
        (i, task) for i, task in enumerate(p.tasks)
        if not (p.HIDE_SUCCESSFUL_TERMINATED_TASKS and task.is_in_successful_terminal_state)
    ]
    total_display = len(display_list)

    try:
        term_height = shutil.get_terminal_size().lines
    except Exception:
        term_height = 24
    max_rows = max(3, term_height - 9)

    selected_display_idx = None
    if tui.selected_task_idx is not None:
        for di, (i, _) in enumerate(display_list):
            if i == tui.selected_task_idx:
                selected_display_idx = di
                break

    if selected_display_idx is not None:
        if selected_display_idx < tui.scroll_offset:
            tui.scroll_offset = selected_display_idx
        elif selected_display_idx >= tui.scroll_offset + max_rows:
            tui.scroll_offset = selected_display_idx - max_rows + 1
    tui.scroll_offset = max(0, min(tui.scroll_offset, max(0, total_display - max_rows)))

    visible = display_list[tui.scroll_offset : tui.scroll_offset + max_rows]
    above   = tui.scroll_offset
    below   = total_display - (tui.scroll_offset + len(visible))

    has_variants = any(task.description for task in p.tasks)
    has_progress = any(
        task.progress is not None and task.progress.total is not None
        for task in p.tasks
    )

    table = Table(
        box=box.SIMPLE_HEAD,
        show_edge=False,
        padding=(0, 1),
        expand=True,
        header_style="bold",
    )
    table.add_column("#",    width=4,  style="dim")
    table.add_column("Name", ratio=2 if has_variants else 1)
    if has_variants:
        table.add_column("Description", ratio=1, style="dim")
    if has_slurm:
        table.add_column("Slurm ID",     width=10)
        table.add_column("Cluster State", width=14)
    if has_progress:
        table.add_column("Progress", width=22)
    table.add_column("Status", width=18)
    table.add_column("Time",   width=6,  justify="right")
    table.add_column("Deps",   width=8,  style="dim")

    status_counts: Dict[str, int] = {}
    for task in p.tasks:
        label = task.status[1].upper()
        status_counts[label] = status_counts.get(label, 0) + 1

    for i, task in visible:
        s, stext, _, slurm_state = task.status
        label = stext.upper()
        sym, style = _STATUS_DISPLAY.get(s, ("?", "dim"))
        badge = Text.assemble((sym + " ", style), (label, style))

        d = task.get_duration()
        duration = f"{d // 60}:{d % 60:02d}" if d else "–"

        deps_str = ",".join(str(t._queue_id) for t in (task.task_dependencies or [])) or "–"
        row_style = "bold reverse" if i == tui.selected_task_idx else None

        variant_cells = [task.description or ""] if has_variants else []

        progress_cells = []
        if has_progress:
            prog = task.progress
            if prog is not None and prog.total is not None:
                frac = prog.fraction or 0.0
                bar_width = 12
                filled = round(frac * bar_width)
                bar = "█" * filled + "░" * (bar_width - filled)
                pct = f"{frac * 100:.0f}%"
                progress_cells = [Text.assemble((bar, "green"), (" ", ""), (pct, "dim"))]
            else:
                progress_cells = [Text("", style="dim")]

        if has_slurm:
            table.add_row(
                str(task.id), task.name, *variant_cells,
                str(task.slurmid or "–"), str(slurm_state or "–"),
                *progress_cells, badge, duration, deps_str,
                style=row_style,
            )
        else:
            table.add_row(
                str(task.id), task.name, *variant_cells,
                *progress_cells, badge, duration, deps_str,
                style=row_style,
            )

    footer = Text()
    if tui.pipeline_done:
        if tui.pipeline_failed:
            footer.append("✗ FAILED", style="bold red")
        else:
            footer.append("✓ DONE", style="bold green")
    elif tui.paused:
        footer.append("⏸ PAUSED", style="bold yellow")
    else:
        footer.append("▶ RUNNING", style="bold green")
    footer.append("  ")
    for label, count in status_counts.items():
        footer.append(f"{count} {label}  ", style="dim")
    footer.append("\n")
    footer.append("↑↓", style="bold cyan")
    footer.append(" select  ", style="dim")
    footer.append("Enter", style="bold cyan")
    footer.append(" detail  ", style="dim")
    footer.append("Esc", style="bold cyan")
    footer.append(" back  ", style="dim")
    if not tui.pipeline_done:
        footer.append("P", style="bold cyan")
        footer.append("/", style="dim")
        footer.append("R", style="bold cyan")
        footer.append(" pause/resume  ", style="dim")
    footer.append("Q", style="bold cyan")
    footer.append(" quit", style="dim")

    if tui.last_command_message and not tui.quit_confirmation_pending:
        from rich.text import Text as RichText
        msg = (RichText.from_markup(tui.last_command_message)
               if '[' in tui.last_command_message
               else RichText(tui.last_command_message, style="italic dim cyan"))
        footer.append("\n")
        footer.append(msg)

    done_tag = ""
    if tui.pipeline_done:
        done_tag = ("  [bold red]FAILED[/bold red]"
                    if tui.pipeline_failed else
                    "  [bold green]DONE[/bold green]")

    group_parts = []
    if above:
        group_parts.append(Text(f"  ▲ {above} more", style="dim"))
    group_parts.append(table)
    if below:
        group_parts.append(Text(f"  ▼ {below} more", style="dim"))
    group_parts.extend([Rule(style="bright_black"), footer])

    if tui.last_command_message and tui.quit_confirmation_pending:
        from rich.text import Text as RichText
        msg = RichText.from_markup(tui.last_command_message)
        msg_panel = Panel(msg, border_style="bold red", padding=(0, 2))
        group_parts.append(msg_panel)

    return Panel(
        Group(*group_parts),
        title=f"[bold]depio[/bold]  [dim]{p.name}[/dim]{done_tag}",
        border_style="bright_black",
        padding=(0, 1),
    )


def render_task_detail(p: "Pipeline") -> Panel:
    tui = p.tui
    task = p.tasks[tui.selected_task_idx]
    s, stext, _, _ = task.status
    sym, style = _STATUS_DISPLAY.get(s, ("?", "dim"))

    d = task.get_duration()
    duration = f"{d // 60}:{d % 60:02d}" if d else "–"

    header = Text.assemble(
        ("# ", "dim"),
        (str(task.id), "bold cyan"),
        ("  ", ""),
        (task.name, "bold"),
        ("  ", ""),
        (sym + " ", style),
        (stext.upper(), style),
        ("  ", "dim"),
        (duration, "dim"),
    )

    try:
        term_height = shutil.get_terminal_size().lines
    except Exception:
        term_height = 24
    stderr_content = task.get_stderr()
    stderr_overhead = 6 if stderr_content else 0
    max_stdout_lines = max(5, term_height - 9 - stderr_overhead)

    has_tail = hasattr(task.stdout, "get_tail")
    if has_tail:
        stdout_content = task.stdout.get_tail(max_stdout_lines)
        total_lines = task.stdout.line_count
        truncated = task.stdout.truncated_lines
    else:
        raw = task.get_stdout() or ""
        lines = raw.split("\n")
        total_lines = len(lines)
        truncated = 0
        stdout_content = "\n".join(lines[-max_stdout_lines:])

    if stdout_content:
        shown_lines = stdout_content.count("\n") + 1
        stdout_body = Text(stdout_content)
    else:
        shown_lines = 0
        stdout_body = Text("(no output)", style="dim")

    hidden = total_lines - shown_lines + truncated
    subtitle = (f"[dim]{hidden:,} earlier lines hidden · showing last {shown_lines}[/dim]"
                if hidden > 0 else None)

    stdout_panel = Panel(
        stdout_body, title="stdout", subtitle=subtitle,
        border_style="dim", padding=(0, 1),
    )

    parts = [header, Rule(style="bright_black")]

    if task.progress is not None:
        snap = task.progress.snapshot()
        phase_prefix = f"[{snap['phase']}]  " if snap["phase"] else ""
        msg_line = f"{phase_prefix}{snap['message']}"
        if snap["total"]:
            frac = min(1.0, snap["current"] / snap["total"])
            bar_width = 40
            filled = round(frac * bar_width)
            bar = "█" * filled + "░" * (bar_width - filled)
            pct_text = f"{snap['current']:,} / {snap['total']:,}  ({frac * 100:.1f}%)"
            progress_body = Text.assemble(
                (bar, "bold green"), ("  ", ""), (pct_text, "dim"), ("\n", ""),
                (msg_line, "italic dim"),
            )
        else:
            progress_body = Text(msg_line or "running…", style="dim")
        parts.append(Panel(progress_body, title="progress",
                           border_style="green dim", padding=(0, 1)))

    parts.append(stdout_panel)

    if stderr_content:
        stderr_lines = stderr_content.split("\n")
        stderr_tail = "\n".join(stderr_lines[-max(5, stderr_overhead - 2):])
        parts.append(Panel(Text(stderr_tail), title="stderr", border_style="red dim", padding=(0, 1)))

    n = len(p.tasks)
    pos = (tui.selected_task_idx or 0) + 1
    footer = Text.assemble(
        ("↑↓", "bold cyan"), (f"  prev/next task ({pos}/{n})  ", "dim"),
        ("Esc", "bold cyan"), ("  back to overview", "dim"),
    )
    parts.extend([Rule(style="bright_black"), footer])

    return Panel(
        Group(*parts),
        title=f"[bold]Task detail[/bold]  [dim]{task.name}[/dim]",
        border_style="cyan",
        padding=(0, 1),
    )
