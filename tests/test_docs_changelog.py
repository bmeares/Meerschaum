"""The website pages remain the complete source for repository release notes."""
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

SPEC = spec_from_file_location(
    'generate_changelog', Path(__file__).resolve().parents[1] / 'scripts/generate_changelog.py',
)
GEN = module_from_spec(SPEC)
SPEC.loader.exec_module(GEN)


def write_pages(source):
    for major in GEN.MAJORS:
        (source / f'v{major}.md').write_text(
            f'# Meerschaum {major}\n\n## {major}.0.0\n\n'
            f'Notes {major}. [Guide](../reference/pipes/index.md#sync).\n\n'
            '```bash\n# Keep code comments unchanged.\nprintf \"[literal](file.md)\"\n```\n\n'
            '<details markdown="1"><summary>More</summary>\n\nArchived text.\n\n</details>\n',
        )


def test_complete_export_preserves_history_order_and_portable_links(tmp_path):
    write_pages(tmp_path)
    rendered = GEN.render_changelog(tmp_path)
    positions = [rendered.index(f'## Meerschaum {major}') for major in GEN.MAJORS]
    assert positions == sorted(positions)
    assert rendered.count('Archived text.') == len(GEN.MAJORS)
    assert rendered.count('https://meerschaum.io/reference/pipes/#sync') == len(GEN.MAJORS)
    assert '### 5.0.0' in rendered
    assert rendered.count('# Keep code comments unchanged.') == len(GEN.MAJORS)
    assert '## Keep code' not in rendered
    assert rendered.count('[literal](file.md)') == len(GEN.MAJORS)
    assert '<details markdown="1">' in rendered


def test_export_refuses_missing_major_instead_of_silently_dropping_history(tmp_path):
    write_pages(tmp_path)
    (tmp_path / 'v2.md').unlink()
    with pytest.raises(FileNotFoundError):
        GEN.render_changelog(tmp_path)


def test_export_refuses_mislabeled_major(tmp_path):
    write_pages(tmp_path)
    (tmp_path / 'v4.md').write_text('# Meerschaum 5\n\nWrong page.\n')
    with pytest.raises(ValueError, match='Meerschaum 4'):
        GEN.render_changelog(tmp_path)
