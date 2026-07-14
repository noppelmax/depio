"""Keyboard input handling for the Pipeline TUI.

All functions receive the Pipeline object so they can read and mutate
its TUI state (pipeline.tui) and call exit helpers.
They are private to the depio package (_-prefixed module).
"""

import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .Pipeline import Pipeline


def read_key() -> str:
    """Read one logical keypress from stdin at the OS level.

    Uses ``os.read()`` instead of ``sys.stdin.read()`` so that
    ``select.select()`` and the actual reads both operate on the OS buffer.
    Python's ``BufferedReader`` would drain all bytes of a multi-byte escape
    sequence in one call, making subsequent ``select()`` checks return False.
    """
    import select
    import os

    fd = sys.stdin.fileno()
    b = os.read(fd, 1)
    if b == b'\x1b':
        if select.select([fd], [], [], 0.05)[0]:
            b2 = os.read(fd, 1)
            if b2 == b'[' and select.select([fd], [], [], 0.05)[0]:
                b3 = os.read(fd, 1)
                return {b'A': 'up', b'B': 'down'}.get(b3, 'esc')
        return 'esc'
    if b in (b'\n', b'\r'):
        return 'enter'
    return b.decode('utf-8', errors='replace')


def check_for_keypress(p: "Pipeline") -> bool:
    """Check stdin for a keypress and update pipeline TUI state.

    Returns ``True`` if a key was handled so the caller can force an
    immediate TUI redraw; ``False`` if no input was available.
    """
    try:
        import select

        if not (hasattr(select, 'select') and hasattr(sys.stdin, 'fileno')):
            return False
        if not select.select([sys.stdin], [], [], 0.0)[0]:
            return False

        key = read_key()
        tui = p.tui
        current_time = time.time()

        if current_time - tui.last_key_press_time > 1.0:
            tui.key_sequence = []
        tui.last_key_press_time = current_time
        tui.key_sequence.append(key)

        if key.lower() == 'p':
            if not tui.pipeline_done:
                tui.paused = True
                tui.last_command_message = "✓ Pipeline paused (press 'r' to resume)"
            tui.quit_confirmation_pending = False
            tui.key_sequence = []

        elif key.lower() == 'r':
            if not tui.pipeline_done:
                tui.paused = False
                tui.last_command_message = "✓ Pipeline resumed"
            tui.quit_confirmation_pending = False
            tui.key_sequence = []

        elif tui.quit_confirmation_pending and key.lower() == 'y':
            tui.last_command_message = "✓ Shutting down..."
            tui.quit_requested = True

        elif tui.quit_confirmation_pending and key.lower() == 'n':
            tui.quit_confirmation_pending = False
            tui.last_command_message = "✓ Quit cancelled"
            tui.key_sequence = []

        elif key.lower() == 'q':
            if tui.pipeline_done:
                tui.quit_requested = True
            else:
                if not tui.quit_confirmation_pending:
                    tui.quit_confirmation_pending = True
                    tui.last_command_message = (
                        "[bold red]Pipeline still running.[/bold red] "
                        "Press [bold yellow]Y[/bold yellow] to confirm quit "
                        "or [bold yellow]N[/bold yellow] to cancel"
                    )
                    tui.key_sequence = []
                else:
                    tui.last_command_message = "✓ Shutting down..."
                    tui.quit_requested = True

        elif key == 'up':
            n = len(p.tasks)
            if n:
                start = 0 if tui.selected_task_idx is None else tui.selected_task_idx
                tui.selected_task_idx = (start - 1) % n

        elif key == 'down':
            n = len(p.tasks)
            if n:
                start = -1 if tui.selected_task_idx is None else tui.selected_task_idx
                tui.selected_task_idx = (start + 1) % n

        elif key == 'enter':
            if tui.selected_task_idx is not None:
                tui.detail_mode = True

        elif key == 'esc':
            if tui.detail_mode:
                tui.detail_mode = False
            else:
                tui.selected_task_idx = None

        return True

    except (ImportError, OSError):
        return False
