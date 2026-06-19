"""Export docs/*.md to PDF via markdown → HTML → xhtml2pdf."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import markdown
from xhtml2pdf import pisa

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

CSS = """
@page { size: A4; margin: 2cm; }
body {
  font-family: Helvetica, Arial, sans-serif;
  font-size: 10pt;
  line-height: 1.45;
  color: #1a1a1a;
}
h1 { font-size: 20pt; margin-top: 0; border-bottom: 2px solid #333; padding-bottom: 6px; }
h2 { font-size: 14pt; margin-top: 18px; border-bottom: 1px solid #ccc; padding-bottom: 4px; }
h3 { font-size: 12pt; margin-top: 14px; }
h4 { font-size: 11pt; margin-top: 10px; }
p, li { margin: 0 0 6px 0; }
ul, ol { margin: 0 0 10px 18px; padding: 0; }
table { border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 9pt; }
th, td { border: 1px solid #bbb; padding: 4px 6px; text-align: left; vertical-align: top; }
th { background: #f0f0f0; }
code { font-family: Consolas, monospace; font-size: 8.5pt; background: #f5f5f5; }
pre {
  background: #f5f5f5;
  border: 1px solid #ddd;
  padding: 8px;
  font-size: 8pt;
  white-space: pre-wrap;
  word-wrap: break-word;
}
pre code { background: transparent; }
blockquote {
  border-left: 3px solid #ccc;
  margin: 8px 0;
  padding: 4px 10px;
  color: #444;
}
hr { border: none; border-top: 1px solid #ccc; margin: 16px 0; }
a { color: #0645ad; text-decoration: none; }
"""


def _md_to_html(text: str) -> str:
    text = re.sub(
        r"```mermaid\n(.*?)```",
        r"<pre><code>[Diagram — see markdown source]\n\1</code></pre>",
        text,
        flags=re.DOTALL,
    )
    return markdown.markdown(
        text,
        extensions=["tables", "fenced_code", "toc", "sane_lists"],
    )


def export_one(src: Path, dest: Path) -> None:
    body = _md_to_html(src.read_text(encoding="utf-8"))
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/>
<style>{CSS}</style></head>
<body>{body}</body></html>"""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as out:
        status = pisa.CreatePDF(html, dest=out, encoding="utf-8")
    if status.err:
        raise RuntimeError(f"PDF generation failed for {src.name}")


def main(argv: list[str]) -> int:
    names = argv or [
        "DEVELOPER_REQUIREMENTS.md",
        "DATABASE_SCHEMA.md",
        "USER_STORIES.md",
    ]
    for name in names:
        src = DOCS / name
        if not src.exists():
            print(f"SKIP missing: {src}", file=sys.stderr)
            continue
        dest = src.with_suffix(".pdf")
        export_one(src, dest)
        print(f"Wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
