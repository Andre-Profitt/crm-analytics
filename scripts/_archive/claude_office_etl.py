#!/usr/bin/env python3
"""Drive the installed Claude Office add-ins as an ETL worker on macOS.

This script uses macOS Accessibility plus Office's native Claude task pane to:

1. Open an Excel workbook or PowerPoint deck.
2. Trigger a Claude skill or paste a prompt.
3. Wait for Claude to enter and exit a running state.
4. Persist the raw pane transcript for downstream processing.

It is intentionally conservative. The Excel add-in is best used for analysis,
while deck authoring should stay in a reproducible script or be treated as an
optional PowerPoint polish step.
"""

from __future__ import annotations

import argparse
import json
import inspect
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DIRECTOR_WORKBOOK_ROOT = REPO_ROOT / "output" / "director_data_dumps"
DIRECTOR_DECK_ROOT = REPO_ROOT / "output" / "sales_director_monthly_runs"
RUN_ROOT = REPO_ROOT / "output" / "claude_office_etl"
DEFAULT_EXECUTIVE_BRIEF_PROMPT = """Read this Sales Director workbook and produce a concise executive deck brief in markdown.

Use exactly these sections and headings:
## Executive Summary
## Momentum Signals
## Risks
## Actions
## Slide Outline

Requirements:
- Be specific to the workbook content and the named territory.
- Use factual metrics from the workbook wherever possible.
- Keep Executive Summary, Momentum Signals, Risks, and Actions to exactly 3 bullets each.
- Make Slide Outline a numbered list of exactly 6 slides for a SimCorp executive review.
- Prefer business insight over workbook QA commentary.
- Mention sheet names inline when they materially support a point.
- Do not ask follow-up questions.
"""


@dataclass(frozen=True)
class OfficeTarget:
    key: str
    app_name: str
    app_path: Path
    default_skill: str
    skill_prompt: str


TARGETS: dict[str, OfficeTarget] = {
    "excel": OfficeTarget(
        key="excel",
        app_name="Microsoft Excel",
        app_path=Path("/Applications/Microsoft Excel.app"),
        default_skill="/audit-xls",
        skill_prompt="audit this workbook for formula, formatting, and data integrity issues",
    ),
    "powerpoint": OfficeTarget(
        key="powerpoint",
        app_name="Microsoft PowerPoint",
        app_path=Path("/Applications/Microsoft PowerPoint.app"),
        default_skill="/ib-check-deck",
        skill_prompt="review for number consistency, data-narrative alignment, language polish, and formatting",
    ),
}

WINDOW_HINTS: dict[str, str | None] = {key: None for key in TARGETS}


class AutomationError(RuntimeError):
    """Raised when the Claude Office automation path cannot continue."""


def _call_with_optional_ensure(func, *args, ensure_pane: bool, **kwargs):
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        signature = None
    if signature and "ensure_pane" in signature.parameters:
        return func(*args, ensure_pane=ensure_pane, **kwargs)
    return func(*args, **kwargs)


@dataclass
class AutomationTrace:
    path: Path
    sequence: int = 0

    def log(self, event: str, **payload: Any) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.sequence += 1
        record = {
            "sequence": self.sequence,
            "timestamp": datetime.now().isoformat(),
            "event": event,
            **payload,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def _timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _latest_dated_dir(root: Path) -> Path:
    dated_dirs = sorted([p for p in root.iterdir() if p.is_dir()])
    if not dated_dirs:
        raise AutomationError(f"No dated directories found under {root}.")
    return dated_dirs[-1]


def _run(command: list[str], *, input_text: str | None = None, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        check=True,
        timeout=timeout,
    )


def _osascript(script: str, *, timeout: int = 30) -> str:
    proc = _run(["osascript", "-e", script], timeout=timeout)
    return proc.stdout.strip()


def _apple_str(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _activate(app_name: str) -> None:
    _osascript(
        f'''
        tell application "{_apple_str(app_name)}" to activate
        delay 1
        '''
    )


def _click_if_exists(app_name: str, expression: str) -> bool:
    try:
        result = _osascript(
            f'''
            tell application "System Events"
              tell process "{_apple_str(app_name)}"
                try
                  if exists {expression} then
                    click {expression}
                    return "clicked"
                  end if
                on error
                  return "missing"
                end try
                return "missing"
              end tell
            end tell
            ''',
        )
        return result == "clicked"
    except subprocess.CalledProcessError:
        return False


def _wait_for_window(target: OfficeTarget, timeout_seconds: int = 20) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = _osascript(
            f'''
            tell application "System Events"
              tell process "{_apple_str(target.app_name)}"
                if (count of windows) is greater than 0 then return "yes"
                return "no"
              end tell
            end tell
            '''
        )
        if result == "yes":
            return
        time.sleep(0.5)
    raise AutomationError(f"No window became available in {target.app_name}.")


def _window_contents(target: OfficeTarget) -> str:
    return _osascript(
        f'''
        tell application "System Events"
          tell process "{_apple_str(target.app_name)}"
            return entire contents of front window
          end tell
        end tell
        ''',
        timeout=60,
    )


def _ribbon_button_expression(target: OfficeTarget, button_name: str) -> str | None:
    window_dump = _window_contents(target)
    match = re.search(
        rf"button {re.escape(button_name)} of group (\d+) of scroll area 1 of tab group 1",
        window_dump,
    )
    if not match:
        return None
    return f'button "{_apple_str(button_name)}" of group {match.group(1)} of scroll area 1 of tab group 1 of front window'


def _open_file(target: OfficeTarget, file_path: Path) -> None:
    if not target.app_path.exists():
        raise AutomationError(f"{target.app_name} is not installed at {target.app_path}.")
    _run(["open", "-a", target.app_name, str(file_path)], timeout=15)
    _activate(target.app_name)
    _wait_for_window(target)
    time.sleep(2)


def _wait_for_named_window(target: OfficeTarget, window_name: str, timeout_seconds: int = 20) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if window_name in _window_names(target):
            return True
        time.sleep(0.5)
    return False


def _restart_powerpoint_process() -> None:
    subprocess.run(
        ["pkill", "-f", "/Applications/Microsoft PowerPoint.app/Contents/MacOS/Microsoft PowerPoint"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    time.sleep(3)


def _dismiss_excel_recovery_if_needed() -> None:
    for window_name in _window_names(TARGETS["excel"]):
        _click_if_exists(
            TARGETS["excel"].app_name,
            f'button "No" of UI element "Open recovered workbooks?" of window "{_apple_str(window_name)}"',
        )


def _pane_exists(target: OfficeTarget) -> bool:
    return _pane_window_name(target) is not None


def _window_names(target: OfficeTarget) -> list[str]:
    output = _osascript(
        f'''
        tell application "System Events"
          tell process "{_apple_str(target.app_name)}"
            if (count of windows) is 0 then return ""
            return name of every window
          end tell
        end tell
        '''
    )
    if not output:
        return []
    return [name.strip() for name in output.split(", ") if name.strip()]


def _set_window_hint(target: OfficeTarget, window_name: str | None) -> None:
    WINDOW_HINTS[target.key] = window_name


def _window_hint(target: OfficeTarget) -> str | None:
    hint = WINDOW_HINTS.get(target.key)
    if not hint:
        return None
    if target.key == "powerpoint":
        return hint
    if hint in _window_names(target):
        return hint
    return None


def _pane_window_name(target: OfficeTarget) -> str | None:
    hint = _window_hint(target)
    if hint:
        result = _osascript(
            f'''
            tell application "System Events"
              tell process "{_apple_str(target.app_name)}"
                try
                  if exists group "Claude" of splitter group 1 of window "{_apple_str(hint)}" then return "yes"
                on error
                  return "no"
                end try
                return "no"
              end tell
            end tell
            '''
        )
        if result == "yes":
            return hint
    for window_name in _window_names(target):
        result = _osascript(
            f'''
            tell application "System Events"
              tell process "{_apple_str(target.app_name)}"
                try
                  if exists group "Claude" of splitter group 1 of window "{_apple_str(window_name)}" then return "yes"
                on error
                  return "no"
                end try
                return "no"
              end tell
            end tell
            '''
        )
        if result == "yes":
            return window_name
    return None


def _pane_present_on_window(target: OfficeTarget, window_name: str) -> bool:
    try:
        result = _osascript(
            f'''
            tell application "System Events"
              tell process "{_apple_str(target.app_name)}"
                try
                  if exists group "Claude" of splitter group 1 of window "{_apple_str(window_name)}" then return "yes"
                on error
                  return "no"
                end try
                return "no"
              end tell
            end tell
            '''
        )
    except subprocess.CalledProcessError:
        return False
    return result == "yes"


def _powerpoint_presentation_names() -> list[str]:
    output = _osascript('tell application "Microsoft PowerPoint" to get name of every presentation')
    if not output:
        return []
    return [name.strip() for name in output.split(", ") if name.strip()]


def _presentation_family_stem(name: str) -> str:
    stem = Path(name).stem
    stem = re.sub(r"\s+-\s+Repaired$", "", stem)
    return re.sub(r" \[build \d{8}-\d{6}\]$", "", stem)


def _close_powerpoint_presentation(name: str, *, save: bool = False) -> bool:
    save_mode = "yes" if save else "no"
    try:
        _osascript(
            f'''
            tell application "Microsoft PowerPoint"
              close presentation "{_apple_str(name)}" saving {save_mode}
            end tell
            ''',
            timeout=30,
        )
    except subprocess.CalledProcessError:
        return False
    return True


def _close_conflicting_powerpoint_presentations(source_file: Path) -> list[str]:
    family_stem = _presentation_family_stem(source_file.name)
    closed: list[str] = []
    for presentation_name in _powerpoint_presentation_names():
        presentation_stem = _presentation_family_stem(presentation_name)
        if presentation_stem != family_stem:
            continue
        if presentation_name == source_file.name:
            continue
        if _close_powerpoint_presentation(presentation_name, save=False):
            closed.append(presentation_name)
    return closed


def _activate_window(target: OfficeTarget, window_name: str) -> None:
    try:
        _osascript(
            f'''
            tell application "{_apple_str(target.app_name)}" to activate
            tell application "System Events"
              tell process "{_apple_str(target.app_name)}"
                if exists window "{_apple_str(window_name)}" then
                  set value of attribute "AXMain" of window "{_apple_str(window_name)}" to true
                end if
              end tell
            end tell
            '''
        )
    except subprocess.CalledProcessError:
        return


def _close_claude_pane_on_window(target: OfficeTarget, window_name: str) -> bool:
    if not _pane_present_on_window(target, window_name):
        return True
    _activate_window(target, window_name)
    close_expressions = [
        f'button "Close Claude" of tab group 1 of group "Claude" of splitter group 1 of window "{_apple_str(window_name)}"',
        f'button "Close Claude" of group "Claude" of splitter group 1 of window "{_apple_str(window_name)}"',
    ]
    for expression in close_expressions:
        if _click_if_exists(target.app_name, expression):
            deadline = time.time() + 5
            while time.time() < deadline:
                if not _pane_present_on_window(target, window_name):
                    return True
                time.sleep(0.25)
    ribbon_expression = _window_ribbon_button_expression(target, "Claude", window_name)
    if ribbon_expression and _click_if_exists(target.app_name, ribbon_expression):
        deadline = time.time() + 5
        while time.time() < deadline:
            if not _pane_present_on_window(target, window_name):
                return True
            time.sleep(0.25)
    return not _pane_present_on_window(target, window_name)


def _looks_like_blank_excel_window(window_name: str) -> bool:
    return bool(re.fullmatch(r"Book\d+", window_name))


def _preferred_excel_window_name() -> str | None:
    for window_name in _window_names(TARGETS["excel"]):
        if window_name == "Office Add-ins":
            continue
        if window_name == "Open new and recent files":
            continue
        if _looks_like_blank_excel_window(window_name):
            continue
        return window_name
    return None


def _preferred_window_name(target: OfficeTarget) -> str | None:
    hint = _window_hint(target)
    if hint:
        return hint
    if target.key == "excel":
        return _preferred_excel_window_name()
    for window_name in _window_names(target):
        return window_name
    return None


def _window_ribbon_button_expression(target: OfficeTarget, button_name: str, window_name: str) -> str | None:
    window_dump = _osascript(
        f'''
        tell application "System Events"
          tell process "{_apple_str(target.app_name)}"
            return entire contents of window "{_apple_str(window_name)}"
          end tell
        end tell
        ''',
        timeout=60,
    )
    match = re.search(
        rf'button {re.escape(button_name)} of group (\d+) of scroll area 1 of tab group 1 of window {re.escape(window_name)}',
        window_dump,
    )
    if not match:
        return None
    return (
        f'button "{_apple_str(button_name)}" of group {match.group(1)} '
        f'of scroll area 1 of tab group 1 of window "{_apple_str(window_name)}"'
    )


def _open_excel_claude_on_window(window_name: str) -> bool:
    _dismiss_excel_recovery_if_needed()
    _activate_window(TARGETS["excel"], window_name)
    ribbon_expression = _window_ribbon_button_expression(TARGETS["excel"], "Claude", window_name)
    if not ribbon_expression:
        return False
    if not _click_if_exists(TARGETS["excel"].app_name, ribbon_expression):
        return False
    deadline = time.time() + 10
    while time.time() < deadline:
        pane_window = _pane_window_name(TARGETS["excel"])
        if pane_window == window_name:
            _activate_window(TARGETS["excel"], window_name)
            return True
        time.sleep(0.5)
    return False


def _excel_addins_window_open() -> bool:
    return "Office Add-ins" in _window_names(TARGETS["excel"])


def _open_excel_addins_dialog() -> bool:
    if _excel_addins_window_open():
        return True
    preferred_window = _preferred_excel_window_name()
    if preferred_window:
        _activate_window(TARGETS["excel"], preferred_window)
    try:
        _osascript(
            '''
            tell application "System Events"
              tell process "Microsoft Excel"
                click menu item "My Add-ins..." of menu "Add-ins" of menu item "Add-ins" of menu "Insert" of menu bar item "Insert" of menu bar 1
              end tell
            end tell
            '''
        )
    except subprocess.CalledProcessError:
        return False
    deadline = time.time() + 12
    while time.time() < deadline:
        if _excel_addins_window_open():
            return True
        time.sleep(0.5)
    return False


def _close_excel_addins_dialog() -> None:
    if not _excel_addins_window_open():
        return
    closed = _click_if_exists(
        TARGETS["excel"].app_name,
        'button "Close" of group 4 of UI element 1 of scroll area 1 of group 1 of group 1 of window "Office Add-ins"',
    )
    if not closed:
        try:
            _osascript(
                '''
                tell application "System Events"
                  tell process "Microsoft Excel"
                    keystroke "w" using command down
                  end tell
                end tell
                '''
            )
        except subprocess.CalledProcessError:
            return


def _launch_excel_claude_via_addins_dialog() -> bool:
    if not _open_excel_addins_dialog():
        return False
    actions = [
        'click button "Add" of group 4 of UI element 1 of scroll area 1 of group 1 of group 1 of window "Office Add-ins"',
        'click static text 1 of list 1 of group 2 of UI element 1 of scroll area 1 of group 1 of group 1 of window "Office Add-ins"',
    ]
    for script in actions:
        try:
            _osascript(
                f'''
                tell application "System Events"
                  tell process "Microsoft Excel"
                    try
                      {script}
                    end try
                  end tell
                end tell
                '''
            )
        except subprocess.CalledProcessError:
            pass
        time.sleep(1)
        pane_window = _pane_window_name(TARGETS["excel"])
        if pane_window:
            _close_excel_addins_dialog()
            _activate_window(TARGETS["excel"], pane_window)
            return True
    try:
        _osascript(
            '''
            tell application "Microsoft Excel" to activate
            tell application "System Events"
              key code 36
            end tell
            '''
        )
    except subprocess.CalledProcessError:
        pass
    time.sleep(1)
    pane_window = _pane_window_name(TARGETS["excel"])
    if pane_window:
        _close_excel_addins_dialog()
        _activate_window(TARGETS["excel"], pane_window)
        return True
    _close_excel_addins_dialog()
    pane_window = _pane_window_name(TARGETS["excel"])
    if pane_window:
        _activate_window(TARGETS["excel"], pane_window)
        return True
    return False


def _create_blank_excel_workbook() -> str | None:
    before = set(_window_names(TARGETS["excel"]))
    try:
        _osascript(
            '''
            tell application "Microsoft Excel" to activate
            tell application "System Events"
              keystroke "n" using command down
            end tell
            '''
        )
    except subprocess.CalledProcessError:
        return None
    deadline = time.time() + 8
    while time.time() < deadline:
        current = _window_names(TARGETS["excel"])
        for window_name in current:
            if window_name not in before and _looks_like_blank_excel_window(window_name):
                return window_name
        time.sleep(0.5)
    for window_name in _window_names(TARGETS["excel"]):
        if _looks_like_blank_excel_window(window_name):
            return window_name
    return None


def _bootstrap_excel_claude_via_blank_workbook() -> bool:
    blank_window = _create_blank_excel_workbook()
    if not blank_window:
        return False
    if not _launch_excel_claude_via_addins_dialog():
        return False
    pane_window = _pane_window_name(TARGETS["excel"])
    preferred_window = _preferred_excel_window_name()
    if preferred_window and pane_window and _looks_like_blank_excel_window(pane_window):
        if _open_excel_claude_on_window(preferred_window):
            return True
    pane_window = _pane_window_name(TARGETS["excel"])
    if pane_window:
        _activate_window(TARGETS["excel"], pane_window)
        return True
    return False


def ensure_claude_pane(target: OfficeTarget, *, trace: AutomationTrace | None = None) -> None:
    _activate(target.app_name)
    _wait_for_window(target)
    if target.key == "excel":
        _dismiss_excel_recovery_if_needed()
    preferred_window = _preferred_window_name(target)
    if preferred_window:
        _activate_window(target, preferred_window)
    if trace:
        trace.log(
            "pane_open_start",
            target=target.key,
            window_names=_window_names(target),
            preferred_window=preferred_window,
        )
    pane_window = _pane_window_name(target)
    if pane_window and (preferred_window is None or pane_window == preferred_window):
        if target.key == "excel" and _looks_like_blank_excel_window(pane_window):
            preferred_window = _preferred_excel_window_name()
            if preferred_window and _open_excel_claude_on_window(preferred_window):
                if trace:
                    trace.log(
                        "pane_open_promoted_blank_window",
                        target=target.key,
                        source_window=pane_window,
                        preferred_window=preferred_window,
                    )
                return
        if target.key == "excel":
            _close_excel_addins_dialog()
        _activate_window(target, pane_window)
        if trace:
            trace.log("pane_open_existing", target=target.key, pane_window=pane_window)
        return
    if pane_window and preferred_window and pane_window != preferred_window and trace:
        trace.log(
            "pane_open_ignoring_mismatched_window",
            target=target.key,
            pane_window=pane_window,
            preferred_window=preferred_window,
        )
    if pane_window and preferred_window and pane_window != preferred_window:
        closed = _close_claude_pane_on_window(target, pane_window)
        if trace:
            trace.log(
                "pane_open_close_mismatched_window",
                target=target.key,
                pane_window=pane_window,
                preferred_window=preferred_window,
                closed=closed,
            )
        _activate_window(target, preferred_window)
        pane_window = _pane_window_name(target)
        if pane_window and (preferred_window is None or pane_window == preferred_window):
            if target.key == "excel":
                _close_excel_addins_dialog()
            _activate_window(target, pane_window)
            if trace:
                trace.log("pane_open_existing_after_rebind", target=target.key, pane_window=pane_window)
            return
    if target.key == "excel":
        preferred_window = _preferred_excel_window_name()
        if preferred_window and _open_excel_claude_on_window(preferred_window):
            if trace:
                trace.log(
                    "pane_open_via_preferred_window_ribbon",
                    target=target.key,
                    preferred_window=preferred_window,
                )
            return
    deadline = time.time() + (60 if target.key == "excel" else 45)
    bootstrap_attempted = False
    iteration = 0
    while time.time() < deadline:
        iteration += 1
        _activate(target.app_name)
        preferred_window = _preferred_window_name(target)
        if preferred_window:
            _activate_window(target, preferred_window)
        if target.key == "excel":
            _dismiss_excel_recovery_if_needed()
            preferred_window = _preferred_excel_window_name()
            if preferred_window and _open_excel_claude_on_window(preferred_window):
                if trace:
                    trace.log(
                        "pane_open_via_preferred_window_ribbon",
                        target=target.key,
                        iteration=iteration,
                        preferred_window=preferred_window,
                    )
                return
        addins_opened = target.key == "excel" and _launch_excel_claude_via_addins_dialog()
        if trace:
            trace.log(
                "pane_open_attempt",
                target=target.key,
                iteration=iteration,
                window_names=_window_names(target),
                addins_opened=bool(addins_opened),
                pane_window=_pane_window_name(target),
                preferred_window=_preferred_window_name(target),
            )
        if addins_opened:
            preferred_window = _preferred_excel_window_name()
            pane_window = _pane_window_name(target)
            if preferred_window and pane_window and _looks_like_blank_excel_window(pane_window):
                if _open_excel_claude_on_window(preferred_window):
                    if trace:
                        trace.log(
                            "pane_open_promoted_blank_window",
                            target=target.key,
                            source_window=pane_window,
                            preferred_window=preferred_window,
                        )
                    return
            if trace:
                trace.log("pane_open_via_addins_dialog", target=target.key, pane_window=pane_window)
            return
        preferred_window = _preferred_window_name(target)
        ribbon_expression = (
            _window_ribbon_button_expression(target, "Claude", preferred_window)
            if preferred_window
            else _ribbon_button_expression(target, "Claude")
        )
        opened = (
            _click_if_exists(target.app_name, ribbon_expression)
            if ribbon_expression
            else False
        )
        if trace:
            trace.log(
                "pane_open_ribbon_attempt",
                target=target.key,
                iteration=iteration,
                ribbon_found=bool(ribbon_expression),
                ribbon_clicked=opened,
            )
        if opened:
            pane_deadline = time.time() + 10
            while time.time() < pane_deadline:
                pane_window = _pane_window_name(target)
                if pane_window and (preferred_window is None or pane_window == preferred_window):
                    pane_window = _pane_window_name(target)
                    if pane_window:
                        _activate_window(target, pane_window)
                    if trace:
                        trace.log("pane_open_via_ribbon", target=target.key, pane_window=pane_window)
                    return
                time.sleep(0.5)
        pane_window = _pane_window_name(target)
        if pane_window and (preferred_window is None or pane_window == preferred_window):
            if target.key == "excel" and _looks_like_blank_excel_window(pane_window):
                preferred_window = _preferred_excel_window_name()
                if preferred_window and _open_excel_claude_on_window(preferred_window):
                    if trace:
                        trace.log(
                            "pane_open_promoted_blank_window",
                            target=target.key,
                            source_window=pane_window,
                            preferred_window=preferred_window,
                        )
                    return
            if target.key == "excel":
                _close_excel_addins_dialog()
            _activate_window(target, pane_window)
            if trace:
                trace.log("pane_open_found_after_attempt", target=target.key, pane_window=pane_window)
            return
        if target.key == "excel" and not bootstrap_attempted and iteration >= 2:
            bootstrap_attempted = True
            bootstrapped = _bootstrap_excel_claude_via_blank_workbook()
            if trace:
                trace.log(
                    "pane_open_blank_bootstrap",
                    target=target.key,
                    iteration=iteration,
                    bootstrapped=bootstrapped,
                    window_names=_window_names(target),
                    pane_window=_pane_window_name(target),
                )
            if bootstrapped:
                return
        time.sleep(1)
    if trace:
        trace.log(
            "pane_open_timeout",
            target=target.key,
            window_names=_window_names(target),
            pane_window=_pane_window_name(target),
            preferred_window=_preferred_window_name(target),
        )
    raise AutomationError(f"Could not find or open the Claude pane in {target.app_name}.")


def dump_claude_pane(target: OfficeTarget, *, ensure_pane: bool = True) -> str:
    if ensure_pane:
        ensure_claude_pane(target)
    preferred_window = _preferred_window_name(target) or _pane_window_name(target)
    if preferred_window:
        try:
            return _osascript(
                f'''
                tell application "System Events"
                  tell process "{_apple_str(target.app_name)}"
                    return entire contents of group "Claude" of splitter group 1 of window "{_apple_str(preferred_window)}"
                  end tell
                end tell
                ''',
                timeout=60,
            )
        except subprocess.CalledProcessError:
            pass
    return _osascript(
        f'''
        tell application "System Events"
          tell process "{_apple_str(target.app_name)}"
            return entire contents of group "Claude" of splitter group 1 of front window
          end tell
        end tell
        ''',
        timeout=60,
    )


def _status_from_dump(dump: str) -> dict[str, bool]:
    return {
        "active_run": (
            ("static text Responding" in dump)
            or ("static text Thinking" in dump)
            or ("button Working" in dump)
        ),
        "running": (
            ("static text Responding" in dump)
            or ("static text Thinking" in dump)
            or ("button Stop" in dump)
            or ("button Working" in dump)
        ),
        "send_ready": 'button Send message' in dump,
        "copy_ready": 'button Copy message' in dump,
        "accept_all_edits": 'button Accept all edits' in dump,
        "ask_before_edits": 'button Ask before edits' in dump,
        "permission_required": "Permission required" in dump,
        "scroll_to_bottom": 'button Scroll to bottom' in dump,
        "new_chat": 'button New chat' in dump,
        "refresh_banner": 'button Refresh' in dump,
        "dismiss_banner": 'button Dismiss' in dump,
        "pane_open": 'group Claude of splitter group 1' in dump,
    }


def _step_progress_from_dump(dump: str) -> dict[str, Any]:
    match = re.search(r"static text Step (\d+) of (\d+)", dump)
    if not match:
        return {
            "step_label": None,
            "step_current": None,
            "step_total": None,
        }
    current = int(match.group(1))
    total = int(match.group(2))
    return {
        "step_label": f"Step {current} of {total}",
        "step_current": current,
        "step_total": total,
    }


def _status_snapshot(target: OfficeTarget, dump: str) -> dict[str, Any]:
    status = _status_from_dump(dump)
    return {
        **status,
        **_step_progress_from_dump(dump),
        "target": target.key,
        "permission_group": _permission_group_from_dump(dump),
        "accept_group": _button_group_from_dump(dump, "Accept all edits"),
        "copy_groups": re.findall(r"button Copy message of group (\d+) of UI element 1 of scroll area 1", dump),
        "dump_length": len(dump),
    }


def _pane_status(target: OfficeTarget) -> dict[str, bool]:
    return _status_from_dump(dump_claude_pane(target))


def _dismiss_pane_banner_if_needed(target: OfficeTarget) -> None:
    _click_if_exists(
        target.app_name,
        'button "Dismiss" of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 of group "Claude" of splitter group 1 of front window',
    )


def _scroll_to_bottom_if_present(target: OfficeTarget) -> bool:
    return _click_if_exists(
        target.app_name,
        'button "Scroll to bottom" of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 of group "Claude" of splitter group 1 of front window',
    )


def _compose_box_present_in_dump(dump: str) -> bool:
    return "text area 1 of UI element 1 of scroll area 1" in dump


def new_chat(target: OfficeTarget, *, ensure_pane: bool = True) -> None:
    if ensure_pane:
        ensure_claude_pane(target)
    for _ in range(10):
        _dismiss_pane_banner_if_needed(target)
        dump = _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=False)
        clicked = _click_if_exists(
            target.app_name,
            'button "New chat" of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 of group "Claude" of splitter group 1 of front window',
        )
        if clicked:
            time.sleep(1)
            return
        if _compose_box_present_in_dump(dump):
            return
        time.sleep(0.5)
    raise AutomationError(f"Could not find the New chat button in {target.app_name}.")


def _pbcopy(text: str) -> None:
    subprocess.run(["pbcopy"], input=text, text=True, check=True)


def _pbpaste() -> str:
    return subprocess.run(
        ["pbpaste"],
        text=True,
        capture_output=True,
        check=True,
    ).stdout


def _compose_box_geometry(target: OfficeTarget) -> tuple[int, int, int, int] | None:
    try:
        raw = _osascript(
            f'''
            tell application "System Events"
              tell process "{_apple_str(target.app_name)}"
                set p to position of text area 1 of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 of group "Claude" of splitter group 1 of front window
                set s to size of text area 1 of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 of group "Claude" of splitter group 1 of front window
                return (item 1 of p as text) & "," & (item 2 of p as text) & ";" & (item 1 of s as text) & "," & (item 2 of s as text)
              end tell
            end tell
            ''',
            timeout=15,
        )
    except subprocess.CalledProcessError:
        return None
    match = re.fullmatch(r"\s*(-?\d+),(-?\d+);(\d+),(\d+)\s*", raw)
    if not match:
        return None
    return tuple(int(part) for part in match.groups())


def _click_screen_point_quartz(x: int, y: int) -> bool:
    try:
        from Quartz import (  # type: ignore[import-not-found]
            CGEventCreateMouseEvent,
            CGEventPost,
            kCGEventLeftMouseDown,
            kCGEventLeftMouseUp,
            kCGHIDEventTap,
            kCGMouseButtonLeft,
        )
    except Exception:  # noqa: BLE001
        return False
    for event_type in (kCGEventLeftMouseDown, kCGEventLeftMouseUp):
        event = CGEventCreateMouseEvent(None, event_type, (x, y), kCGMouseButtonLeft)
        CGEventPost(kCGHIDEventTap, event)
        time.sleep(0.05)
    return True


def _click_compose_box_with_quartz(target: OfficeTarget) -> bool:
    geometry = _compose_box_geometry(target)
    if geometry is None:
        return False
    x, y, width, height = geometry
    click_x = x + max(12, min(width // 4, 48))
    click_y = y + max(8, height // 2)
    _activate(target.app_name)
    return _click_screen_point_quartz(click_x, click_y)


def click_skill(target: OfficeTarget, skill_name: str, *, ensure_pane: bool = True) -> None:
    if ensure_pane:
        ensure_claude_pane(target)
    clicked = _click_if_exists(
        target.app_name,
        f'button "{_apple_str(skill_name)}" of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 of group "Claude" of splitter group 1 of front window',
    )
    if not clicked:
        raise AutomationError(f"Skill button {skill_name!r} was not found in {target.app_name}.")
    time.sleep(1)


def paste_prompt(
    target: OfficeTarget,
    prompt: str,
    trace: AutomationTrace | None = None,
    *,
    ensure_pane: bool = True,
) -> None:
    if ensure_pane:
        ensure_claude_pane(target, trace=trace)
    _pbcopy(prompt)
    used_quartz_focus = target.key == "excel" and _click_compose_box_with_quartz(target)
    if trace:
        trace.log(
            "prompt_focus_attempt",
            target=target.key,
            focus_mode="quartz" if used_quartz_focus else "ax",
            quartz_success=used_quartz_focus,
        )
    if used_quartz_focus:
        _osascript(
            f'''
            tell application "{_apple_str(target.app_name)}" to activate
            delay 0.2
            tell application "System Events"
              keystroke "a" using {{command down}}
              delay 0.1
              keystroke "v" using {{command down}}
            end tell
            ''',
            timeout=30,
        )
    else:
        _osascript(
            f'''
            tell application "{_apple_str(target.app_name)}" to activate
            delay 0.3
            tell application "System Events"
              tell process "{_apple_str(target.app_name)}"
                set focused of text area 1 of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 of group "Claude" of splitter group 1 of front window to true
              end tell
              delay 0.2
              keystroke "a" using {{command down}}
              delay 0.1
              keystroke "v" using {{command down}}
            end tell
            ''',
            timeout=30,
        )
    time.sleep(1)
    if trace:
        dump = _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=False)
        trace.log(
            "prompt_paste_status",
            focus_mode="quartz" if used_quartz_focus else "ax",
            **_status_snapshot(target, dump),
        )


def _keyboard_send_message(target: OfficeTarget, *, command_modifier: bool = False) -> None:
    modifier = " using {command down}" if command_modifier else ""
    _osascript(
        f'''
        tell application "{_apple_str(target.app_name)}" to activate
        delay 0.2
        tell application "System Events"
          key code 36{modifier}
        end tell
        ''',
        timeout=15,
    )


def send_message(
    target: OfficeTarget,
    timeout_seconds: int = 10,
    *,
    trace: AutomationTrace | None = None,
    ensure_pane: bool = True,
) -> None:
    if ensure_pane:
        ensure_claude_pane(target)
    fallback_used: set[str] = set()
    iteration = 0
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        iteration += 1
        dump = _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=False)
        status = _status_snapshot(target, dump)
        clicked = _click_if_exists(
            target.app_name,
            'button "Send message" of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 of group "Claude" of splitter group 1 of front window',
        )
        if trace:
            trace.log(
                "send_message_poll",
                iteration=iteration,
                send_button_clicked=clicked,
                **status,
            )
        if clicked:
            return
        if "return" not in fallback_used:
            _keyboard_send_message(target)
            fallback_used.add("return")
            time.sleep(1)
            dump_after = _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=False)
            status_after = _status_snapshot(target, dump_after)
            if trace:
                trace.log(
                    "send_message_keyboard_fallback",
                    iteration=iteration,
                    variant="return",
                    **status_after,
                )
            if status_after["running"]:
                return
        elif "cmd-return" not in fallback_used:
            _keyboard_send_message(target, command_modifier=True)
            fallback_used.add("cmd-return")
            time.sleep(1)
            dump_after = _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=False)
            status_after = _status_snapshot(target, dump_after)
            if trace:
                trace.log(
                    "send_message_keyboard_fallback",
                    iteration=iteration,
                    variant="cmd-return",
                    **status_after,
                )
            if status_after["running"]:
                return
        time.sleep(0.5)
    raise AutomationError(f"Send message button was not available in {target.app_name}.")


def wait_for_run_start(target: OfficeTarget, timeout_seconds: int = 30) -> None:
    wait_for_run_start_with_trace(target, timeout_seconds=timeout_seconds, trace=None)


def wait_for_run_start_with_trace(
    target: OfficeTarget,
    *,
    timeout_seconds: int = 30,
    trace: AutomationTrace | None,
    ensure_pane: bool = True,
) -> None:
    deadline = time.time() + timeout_seconds
    iteration = 0
    while time.time() < deadline:
        iteration += 1
        dump = _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=ensure_pane)
        status = _status_snapshot(target, dump)
        if trace:
            trace.log("wait_start_poll", iteration=iteration, **status)
        if status["running"]:
            if trace:
                trace.log("wait_start_ready", iteration=iteration, **status)
            return
        time.sleep(1)
    if trace:
        trace.log("wait_start_timeout", iteration=iteration)
    raise AutomationError(f"Claude did not enter a running state in {target.app_name}.")


def _permission_group_from_dump(dump: str) -> int | None:
    match = re.search(r"UI element Permission required of group (\d+)", dump)
    return int(match.group(1)) if match else None


def _click_permission_button(target: OfficeTarget, group_index: int, button_name: str) -> bool:
    return _click_if_exists(
        target.app_name,
        (
            f'button "{_apple_str(button_name)}" of group {group_index} '
            'of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 '
            'of group "Claude" of splitter group 1 of front window'
        ),
    )


def _button_group_from_dump(dump: str, button_name: str) -> int | None:
    match = re.search(
        rf"button {re.escape(button_name)} of group (\d+) of UI element 1 of scroll area 1",
        dump,
    )
    return int(match.group(1)) if match else None


def _click_dynamic_pane_button(target: OfficeTarget, dump: str, button_name: str) -> bool:
    group_index = _button_group_from_dump(dump, button_name)
    if group_index is None:
        return False
    return _click_if_exists(
        target.app_name,
        (
            f'button "{_apple_str(button_name)}" of group {group_index} '
            'of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 '
            'of group "Claude" of splitter group 1 of front window'
        ),
    )


def _handle_permission_prompt(target: OfficeTarget, dump: str, edit_permission_mode: str) -> str | None:
    group_index = _permission_group_from_dump(dump)
    if group_index is None:
        return None
    if edit_permission_mode == "ask":
        raise AutomationError(
            f"Claude is waiting for edit permission in {target.app_name}; rerun with edit permission enabled."
        )
    buttons = (
        ["Always allow ⌘⏎", "Allow once ⏎"]
        if edit_permission_mode == "always-allow"
        else ["Allow once ⏎", "Always allow ⌘⏎"]
    )
    for button_name in buttons:
        if _click_permission_button(target, group_index, button_name):
            time.sleep(1)
            return button_name
    raise AutomationError(f"Edit permission prompt was present in {target.app_name}, but no approval button was clickable.")


def _accept_all_edits_if_present(target: OfficeTarget, dump: str) -> bool:
    return _click_dynamic_pane_button(target, dump, "Accept all edits")


def _terminal_ready_status(status: dict[str, Any], *, stable_count: int) -> bool:
    # PowerPoint can expose stale "Responding/Stop" controls after the run has
    # effectively finished. Treat a stable pane with both send/copy affordances
    # as terminal enough for the control plane to move on.
    if status["copy_ready"] and status["send_ready"] and stable_count >= 3:
        return True
    if not status["active_run"] and (status["copy_ready"] or status["send_ready"] or stable_count >= 3):
        return True
    return False


def wait_for_run_finish(
    target: OfficeTarget,
    timeout_seconds: int = 300,
    *,
    edit_permission_mode: str = "ask",
    trace: AutomationTrace | None = None,
    ensure_pane: bool = True,
) -> None:
    deadline = time.time() + timeout_seconds
    saw_running = False
    stable_dump = None
    stable_count = 0
    iteration = 0
    last_status: dict[str, Any] | None = None
    pane_rebind_attempts = 0
    while time.time() < deadline:
        iteration += 1
        try:
            dump = _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=ensure_pane)
        except Exception as exc:
            if (
                last_status is not None
                and saw_running
                and _is_no_window_error(exc, target)
                and _terminal_ready_status(last_status, stable_count=stable_count)
            ):
                if trace:
                    trace.log(
                        "wait_finish_window_loss_after_terminal_state",
                        iteration=iteration,
                        saw_running=saw_running,
                        stable_count=stable_count,
                        edit_permission_mode=edit_permission_mode,
                        **last_status,
                    )
                return
            if saw_running and _is_pane_access_error(exc, target) and pane_rebind_attempts < 3:
                pane_rebind_attempts += 1
                rebound = False
                rebound_error: Exception | None = None
                try:
                    ensure_claude_pane(target, trace=trace)
                    rebound = True
                except Exception as rebind_exc:  # noqa: BLE001
                    rebound_error = rebind_exc
                if trace:
                    trace.log(
                        "wait_finish_pane_rebind_attempt",
                        iteration=iteration,
                        saw_running=saw_running,
                        stable_count=stable_count,
                        attempt=pane_rebind_attempts,
                        rebound=rebound,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        rebind_error_type=type(rebound_error).__name__ if rebound_error else None,
                        rebind_error_message=str(rebound_error) if rebound_error else None,
                    )
                if rebound:
                    stable_dump = None
                    stable_count = 0
                    time.sleep(1)
                    continue
            raise
        status = _status_snapshot(target, dump)
        last_status = status
        saw_running = saw_running or status["running"]
        if trace:
            trace.log(
                "wait_finish_poll",
                iteration=iteration,
                saw_running=saw_running,
                stable_count=stable_count,
                edit_permission_mode=edit_permission_mode,
                **status,
            )
        if status["permission_required"]:
            clicked = _handle_permission_prompt(target, dump, edit_permission_mode)
            if trace:
                trace.log(
                    "wait_finish_permission_handled",
                    iteration=iteration,
                    clicked=clicked,
                    **status,
                )
            stable_dump = None
            stable_count = 0
            saw_running = True
            time.sleep(2)
            continue
        # "Accept all edits" is a persistent edit-mode toggle in the PowerPoint
        # pane, not a one-shot approval gate. Auto-clicking it on every poll can
        # flip the mode back and forth and trap the run in a self-inflicted loop.
        if saw_running and not status["running"] and status["scroll_to_bottom"] and not status["copy_ready"]:
            scrolled = _scroll_to_bottom_if_present(target)
            if trace:
                trace.log(
                    "wait_finish_scroll_to_bottom",
                    iteration=iteration,
                    clicked=scrolled,
                    **status,
                )
            if scrolled:
                stable_dump = None
                stable_count = 0
                time.sleep(1)
                continue
        if dump == stable_dump:
            stable_count += 1
        else:
            stable_dump = dump
            stable_count = 1
        if saw_running and _terminal_ready_status(status, stable_count=stable_count):
            if trace:
                trace.log(
                    "wait_finish_idle",
                    iteration=iteration,
                    saw_running=saw_running,
                    stable_count=stable_count,
                    **status,
                )
            return
        time.sleep(2)
    if trace:
        trace.log(
            "wait_finish_timeout",
            iteration=iteration,
            saw_running=saw_running,
            stable_count=stable_count,
            edit_permission_mode=edit_permission_mode,
        )
    raise AutomationError(f"Claude did not return to an idle state in {target.app_name}.")


def save_transcript(target: OfficeTarget, output_path: Path, *, ensure_pane: bool = True) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=ensure_pane),
        encoding="utf-8",
    )
    return output_path


def save_transcript_placeholder(output_path: Path, message: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(message.rstrip() + "\n", encoding="utf-8")
    return output_path


def save_summary(output_path: Path, payload: dict[str, object]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def save_text(output_path: Path, text: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path


def copy_latest_message(target: OfficeTarget) -> str | None:
    return copy_latest_message_with_trace(target, trace=None)


def copy_latest_message_with_trace(
    target: OfficeTarget,
    trace: AutomationTrace | None,
    *,
    ensure_pane: bool = True,
) -> str | None:
    if ensure_pane:
        ensure_claude_pane(target, trace=trace)
    dump = _call_with_optional_ensure(dump_claude_pane, target, ensure_pane=False)
    group_matches = re.findall(r"button Copy message of group (\d+) of UI element 1 of scroll area 1", dump)
    if trace:
        trace.log(
            "copy_latest_message_probe",
            group_matches=group_matches,
            **_status_snapshot(target, dump),
        )
    if not group_matches:
        return None
    clicked = _click_if_exists(
        target.app_name,
        (
            f'button "Copy message" of group {group_matches[-1]} '
            'of UI element 1 of scroll area 1 of group 1 of group 1 of group 1 '
            'of group "Claude" of splitter group 1 of front window'
        ),
    )
    if not clicked:
        if trace:
            trace.log("copy_latest_message_click_failed", group=group_matches[-1])
        return None
    time.sleep(0.5)
    copied = _pbpaste().strip()
    if trace:
        trace.log("copy_latest_message_success", group=group_matches[-1], copied_chars=len(copied))
    return copied


def save_open_document(target: OfficeTarget) -> None:
    _osascript(
        f'''
        tell application "{_apple_str(target.app_name)}" to activate
        delay 0.3
        tell application "System Events"
          keystroke "s" using {{command down}}
        end tell
        ''',
        timeout=15,
    )
    time.sleep(1)


def _match_director_file(root: Path, director_name: str, suffix: str) -> Path:
    matches = sorted(
        [
            path
            for path in root.iterdir()
            if path.is_file() and path.suffix == suffix and director_name.lower() in path.name.lower()
        ]
    )
    if not matches:
        raise AutomationError(f"No {suffix} file for {director_name!r} found in {root}.")
    return matches[0]


def _copy_deck_for_editing(deck_path: Path, run_dir: Path) -> Path:
    editable_dir = run_dir / "editable_decks"
    editable_dir.mkdir(parents=True, exist_ok=True)
    destination = editable_dir / deck_path.name
    shutil.copy2(deck_path, destination)
    return destination


def _director_prompt(prompt_template: str, *, director_name: str, workbook_path: Path) -> str:
    territory = workbook_path.stem.split("(")[-1].rstrip(")") if "(" in workbook_path.stem else ""
    context = [
        f"Director: {director_name}",
        f"Workbook: {workbook_path.name}",
    ]
    if territory:
        context.append(f"Territory: {territory}")
    return "\n".join(context) + "\n\n" + prompt_template.strip() + "\n"


def _file_metadata(path: Path) -> dict[str, float | int] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return {
        "mtime": stat.st_mtime,
        "size": stat.st_size,
    }


def _is_no_window_error(exc: Exception, target: OfficeTarget) -> bool:
    return isinstance(exc, AutomationError) and str(exc) == f"No window became available in {target.app_name}."


def _is_pane_access_error(exc: Exception, target: OfficeTarget) -> bool:
    if isinstance(exc, AutomationError) and (
        str(exc) == f"Could not find or open the Claude pane in {target.app_name}."
    ):
        return True
    if isinstance(exc, subprocess.CalledProcessError):
        message = " ".join(
            part
            for part in (
                str(exc),
                exc.stdout if isinstance(exc.stdout, str) else "",
                exc.stderr if isinstance(exc.stderr, str) else "",
            )
            if part
        )
        return (
            'group "Claude" of splitter group 1' in message
            or f'process "{target.app_name}"' in message
            or "front window" in message
        )
    return False


def _recover_completed_powerpoint_window_loss(
    *,
    target: OfficeTarget,
    source_file: Path,
    source_file_before: dict[str, float | int] | None,
    trace: AutomationTrace,
) -> dict[str, object] | None:
    if target.key != "powerpoint":
        return None
    source_file_after = _file_metadata(source_file)
    if source_file_after is None:
        trace.log(
            "window_loss_recovery_unavailable",
            reason="source_file_missing",
            source_file=str(source_file),
        )
        return None
    before_mtime = source_file_before["mtime"] if source_file_before else None
    after_mtime = source_file_after["mtime"]
    mtime_advanced = before_mtime is not None and after_mtime > float(before_mtime) + 1e-6
    if not mtime_advanced:
        trace.log(
            "window_loss_recovery_unavailable",
            reason="source_file_not_modified",
            source_file=str(source_file),
            before_mtime=before_mtime,
            after_mtime=after_mtime,
            source_file_size=source_file_after["size"],
        )
        return None
    trace.log(
        "window_loss_recovered",
        source_file=str(source_file),
        before_mtime=before_mtime,
        after_mtime=after_mtime,
        source_file_size=source_file_after["size"],
    )
    return {
        "message_copied": False,
        "window_loss_recovered": True,
        "source_file_mtime_before": before_mtime,
        "source_file_mtime_after": after_mtime,
        "source_file_size": source_file_after["size"],
    }


def run_skill(
    target: OfficeTarget,
    *,
    source_file: Path,
    skill_name: str | None,
    prompt: str | None,
    wait_finish_seconds: int,
    run_dir: Path,
    edit_permission_mode: str = "ask",
    save_document_on_finish: bool = False,
) -> dict[str, object]:
    run_dir.mkdir(parents=True, exist_ok=True)
    debug_path = run_dir / f"{target.key}-debug.jsonl"
    trace = AutomationTrace(debug_path)
    source_file_before = _file_metadata(source_file)
    if target.key == "powerpoint":
        closed_presentations = _close_conflicting_powerpoint_presentations(source_file)
        if closed_presentations:
            trace.log(
                "powerpoint_conflicting_presentations_closed",
                source_file=str(source_file),
                closed_presentations=closed_presentations,
            )
    _set_window_hint(target, source_file.stem)
    trace.log(
        "run_skill_start",
        target=target.key,
        source_file=str(source_file),
        window_hint=source_file.stem,
        skill_name=skill_name,
        prompt_supplied=bool(prompt),
        wait_finish_seconds=wait_finish_seconds,
        edit_permission_mode=edit_permission_mode,
        save_document_on_finish=save_document_on_finish,
        source_file_before=source_file_before,
    )
    _open_file(target, source_file)
    if target.key == "powerpoint":
        named_window_ready = _wait_for_named_window(target, source_file.stem, timeout_seconds=20)
        trace.log(
            "powerpoint_named_window_check",
            expected_window=source_file.stem,
            named_window_ready=named_window_ready,
            window_names=_window_names(target),
        )
        if not named_window_ready:
            trace.log(
                "powerpoint_named_window_retry_restart",
                expected_window=source_file.stem,
                window_names=_window_names(target),
            )
            _restart_powerpoint_process()
            _open_file(target, source_file)
            named_window_ready = _wait_for_named_window(target, source_file.stem, timeout_seconds=20)
            trace.log(
                "powerpoint_named_window_check_retry",
                expected_window=source_file.stem,
                named_window_ready=named_window_ready,
                window_names=_window_names(target),
            )
            if not named_window_ready:
                raise AutomationError(
                    f"Opened PowerPoint file did not surface expected window {source_file.stem}."
                )
    trace.log("file_opened", target=target.key, source_file=str(source_file))
    if target.key == "excel":
        _dismiss_excel_recovery_if_needed()
        trace.log("excel_recovery_checked")
    ensure_claude_pane(target, trace=trace)
    trace.log("pane_ready")
    _call_with_optional_ensure(new_chat, target, ensure_pane=False)
    trace.log("new_chat_clicked")
    if skill_name:
        _call_with_optional_ensure(click_skill, target, skill_name, ensure_pane=False)
        trace.log("skill_clicked", skill_name=skill_name)
    if prompt:
        _call_with_optional_ensure(paste_prompt, target, prompt, trace=trace, ensure_pane=False)
        trace.log("prompt_pasted", prompt_chars=len(prompt))
    _call_with_optional_ensure(send_message, target, trace=trace, ensure_pane=False)
    trace.log("message_sent")
    message_path = run_dir / f"{target.key}-message.txt"
    transcript_path = run_dir / f"{target.key}-transcript.txt"
    result: dict[str, object] = {
        "app": target.key,
        "source_file": str(source_file),
        "skill": skill_name,
        "prompt_supplied": bool(prompt),
        "debug_path": str(debug_path),
    }
    if prompt:
        prompt_path = save_text(run_dir / f"{target.key}-prompt.txt", prompt + "\n")
        result["prompt_path"] = str(prompt_path)
    run_error: Exception | None = None
    run_finished = False
    try:
        _call_with_optional_ensure(
            wait_for_run_start_with_trace,
            target,
            timeout_seconds=30,
            trace=trace,
            ensure_pane=False,
        )
        _call_with_optional_ensure(
            wait_for_run_finish,
            target,
            timeout_seconds=wait_finish_seconds,
            edit_permission_mode=edit_permission_mode,
            trace=trace,
            ensure_pane=False,
        )
        run_finished = True
        if save_document_on_finish:
            save_open_document(target)
            trace.log("document_saved")
        copied_message = _call_with_optional_ensure(
            copy_latest_message_with_trace,
            target,
            trace,
            ensure_pane=False,
        )
        if copied_message:
            message_path.write_text(copied_message + "\n", encoding="utf-8")
            result["message_path"] = str(message_path)
            result["message_copied"] = True
            trace.log("message_written", message_path=str(message_path), copied_chars=len(copied_message))
        else:
            result["message_copied"] = False
            trace.log("message_missing")
        trace.log("run_skill_success", message_copied=result["message_copied"])
    except Exception as exc:
        run_error = exc
        trace.log(
            "run_skill_error",
            error_type=type(exc).__name__,
            error_message=str(exc),
            run_finished=run_finished,
        )
        if run_finished and _is_no_window_error(exc, target):
            recovered = _recover_completed_powerpoint_window_loss(
                target=target,
                source_file=source_file,
                source_file_before=source_file_before,
                trace=trace,
            )
            if recovered is not None:
                result.update(recovered)
                trace.log(
                    "run_skill_success",
                    message_copied=result["message_copied"],
                    recovered_after_window_loss=True,
                )
                run_error = None
    finally:
        try:
            _call_with_optional_ensure(save_transcript, target, transcript_path, ensure_pane=False)
            trace.log("transcript_saved", transcript_path=str(transcript_path), placeholder=False)
        except Exception as transcript_exc:  # noqa: BLE001
            note = (
                "[transcript unavailable]\n"
                f"type: {type(transcript_exc).__name__}\n"
                f"message: {transcript_exc}\n"
            )
            save_transcript_placeholder(transcript_path, note)
            result["transcript_error"] = {
                "type": type(transcript_exc).__name__,
                "message": str(transcript_exc),
            }
            trace.log(
                "transcript_save_error",
                transcript_path=str(transcript_path),
                error_type=type(transcript_exc).__name__,
                error_message=str(transcript_exc),
            )
            trace.log("transcript_saved", transcript_path=str(transcript_path), placeholder=True)
        result["transcript_path"] = str(transcript_path)
        result["edit_permission_mode"] = edit_permission_mode
        result["save_document_on_finish"] = save_document_on_finish
        _set_window_hint(target, None)
    if run_error is not None:
        raise run_error
    return result


def run_director(args: argparse.Namespace) -> int:
    workbook_root = Path(args.workbook_root) / args.workbook_date if args.workbook_date else _latest_dated_dir(Path(args.workbook_root))
    deck_root = Path(args.deck_root) / args.deck_date if args.deck_date else _latest_dated_dir(Path(args.deck_root))
    workbook_path = _match_director_file(workbook_root, args.director, ".xlsx")
    baseline_deck_path = _match_director_file(deck_root, args.director, ".pptx")
    run_dir = Path(args.run_root) / _timestamp_slug() / args.director.replace(" ", "_")
    run_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, object] = {
        "director": args.director,
        "workbook_path": str(workbook_path),
        "baseline_deck_path": str(baseline_deck_path),
        "run_dir": str(run_dir),
        "started_at": datetime.now().isoformat(),
    }

    if not args.skip_excel_audit:
        excel_audit_result = run_skill(
            TARGETS["excel"],
            source_file=workbook_path,
            skill_name=args.excel_skill,
            prompt=args.excel_prompt,
            wait_finish_seconds=args.excel_timeout,
            run_dir=run_dir / "excel_audit",
        )
        summary["excel_audit"] = excel_audit_result

    if not args.skip_excel_brief:
        brief_prompt = _director_prompt(
            args.excel_brief_prompt,
            director_name=args.director,
            workbook_path=workbook_path,
        )
        excel_brief_result = run_skill(
            TARGETS["excel"],
            source_file=workbook_path,
            skill_name=None,
            prompt=brief_prompt,
            wait_finish_seconds=args.excel_timeout,
            run_dir=run_dir / "excel_brief",
        )
        summary["excel_brief"] = excel_brief_result

    if not args.skip_powerpoint:
        editable_deck_path = _copy_deck_for_editing(baseline_deck_path, run_dir)
        powerpoint_result = run_skill(
            TARGETS["powerpoint"],
            source_file=editable_deck_path,
            skill_name=args.powerpoint_skill,
            prompt=args.powerpoint_prompt,
            wait_finish_seconds=args.powerpoint_timeout,
            run_dir=run_dir / "powerpoint_review",
        )
        summary["powerpoint"] = {
            **powerpoint_result,
            "editable_deck_path": str(editable_deck_path),
        }

    summary["finished_at"] = datetime.now().isoformat()
    save_summary(run_dir / "run-summary.json", summary)
    print(json.dumps(summary, indent=2))
    return 0


def dump_pane_command(args: argparse.Namespace) -> int:
    target = TARGETS[args.app]
    if args.file:
        _open_file(target, Path(args.file))
        if target.key == "excel":
            _dismiss_excel_recovery_if_needed()
    output_path = Path(args.output) if args.output else Path(args.run_root) / f"{target.key}-pane-{_timestamp_slug()}.txt"
    save_transcript(target, output_path)
    print(output_path)
    return 0


def run_skill_command(args: argparse.Namespace) -> int:
    target = TARGETS[args.app]
    source_file = Path(args.file)
    run_dir = Path(args.run_root) / f"{target.key}-{_timestamp_slug()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    result = run_skill(
        target,
        source_file=source_file,
        skill_name=args.skill,
        prompt=args.prompt,
        wait_finish_seconds=args.timeout,
        run_dir=run_dir,
        edit_permission_mode=args.edit_permission_mode,
        save_document_on_finish=args.save_document_on_finish,
    )
    save_summary(run_dir / "result.json", result)
    print(json.dumps(result, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    dump_parser = subparsers.add_parser("dump-pane", help="Capture the raw Claude pane tree from Excel or PowerPoint.")
    dump_parser.add_argument("--app", choices=sorted(TARGETS.keys()), required=True)
    dump_parser.add_argument("--file", help="Optional file to open before dumping the Claude pane.")
    dump_parser.add_argument("--output", help="Optional output path for the raw transcript dump.")
    dump_parser.add_argument("--run-root", default=str(RUN_ROOT))
    dump_parser.set_defaults(func=dump_pane_command)

    skill_parser = subparsers.add_parser("run-skill", help="Open a file, trigger a Claude skill or prompt, and persist the transcript.")
    skill_parser.add_argument("--app", choices=sorted(TARGETS.keys()), required=True)
    skill_parser.add_argument("--file", required=True, help="Workbook or deck to open in the target Office app.")
    skill_parser.add_argument("--skill", help="Skill button to click, for example /audit-xls or /ib-check-deck.")
    skill_parser.add_argument("--prompt", help="Optional prompt text to paste before sending.")
    skill_parser.add_argument("--timeout", type=int, default=300, help="Seconds to wait for Claude to finish.")
    skill_parser.add_argument(
        "--edit-permission-mode",
        choices=("ask", "allow-once", "always-allow"),
        default="ask",
        help="How to handle in-app Claude edit permission prompts.",
    )
    skill_parser.add_argument(
        "--save-document-on-finish",
        action="store_true",
        help="Send Command-S after Claude returns to idle.",
    )
    skill_parser.add_argument("--run-root", default=str(RUN_ROOT))
    skill_parser.set_defaults(func=run_skill_command)

    director_parser = subparsers.add_parser(
        "run-director",
        help="Run the Excel analysis lane for a director and optionally the PowerPoint lane on a copied deck.",
    )
    director_parser.add_argument("--director", required=True, help="Director name fragment, for example 'Adam Steinhaus'.")
    director_parser.add_argument("--workbook-root", default=str(DIRECTOR_WORKBOOK_ROOT))
    director_parser.add_argument("--deck-root", default=str(DIRECTOR_DECK_ROOT))
    director_parser.add_argument("--workbook-date", help="Explicit workbook date directory, for example 2026-04-10.")
    director_parser.add_argument("--deck-date", help="Explicit deck date directory, for example 2026-04-09.")
    director_parser.add_argument("--excel-skill", default=TARGETS["excel"].default_skill)
    director_parser.add_argument("--excel-prompt", default=None)
    director_parser.add_argument("--excel-brief-prompt", default=DEFAULT_EXECUTIVE_BRIEF_PROMPT)
    director_parser.add_argument("--skip-excel-audit", action="store_true")
    director_parser.add_argument("--skip-excel-brief", action="store_true")
    director_parser.add_argument("--powerpoint-skill", default=TARGETS["powerpoint"].default_skill)
    director_parser.add_argument("--powerpoint-prompt", default=None)
    director_parser.add_argument("--excel-timeout", type=int, default=300)
    director_parser.add_argument("--powerpoint-timeout", type=int, default=300)
    director_parser.add_argument("--skip-powerpoint", action="store_true")
    director_parser.add_argument("--run-root", default=str(RUN_ROOT))
    director_parser.set_defaults(func=run_director)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
