from __future__ import annotations

from scripts.export_powerpoint_pdf import build_export_applescript


def test_build_export_applescript_uses_posix_file_for_output() -> None:
    script = build_export_applescript()

    assert 'set outputFile to POSIX file outputPath' in script
    assert 'save p in outputFile as save as PDF' in script
    assert 'delay 2' in script
