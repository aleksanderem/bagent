"""Email renderer — wraps copywriter markdown in BeautyAudit visual shell.

Inputs:
  - body_md: markdown content from
    docs/outreach-assets/funnels/<funnel>/templates/*.md (the copywriter
    writes copy with {{variable}} placeholders intact)
  - subject: rendered at template create time
  - preview_text: 1 liner shown in inbox row
  - utm_campaign: passed through to header logo link for attribution

Outputs:
  - body_html: full <!DOCTYPE html>... ready to ship to wintact via
    /templates.create. {{variable}} placeholders survive — wintact's
    own compile_template substitutes them at send time.

Shell + footer files live in BEAUTY_AUDIT repo at
docs/outreach-assets/shared/{email-shell.html,footer-template.html}.
That repo path is operator-controlled via OUTREACH_ASSETS_DIR env var
(falls back to a sensible default for dev).

Determinism: same input always produces same output. The renderer is
stateless and the templates are committed to git, so two operators
running the loader against the same MD file get byte-identical HTML —
which is what makes "preview in approval queue == what wintact gets"
guarantee work.
"""

from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from pathlib import Path

import mistune

logger = logging.getLogger("bagent.services.email_renderer")


# Sentinel placeholders kept in the email-shell.html template — these
# get substituted with rendered content. Anything looking like
# {{variable}} that isn't one of these is left intact for wintact to
# substitute at send time.
SHELL_PLACEHOLDERS = {"{{SUBJECT}}", "{{PREVIEW_TEXT}}", "{{BODY_HTML}}", "{{FOOTER_HTML}}"}


def _assets_dir() -> Path:
    """Resolve docs/outreach-assets/ path.

    Order: env var → conventional dev path → raise. We don't try to be
    clever about discovering it from cwd because bagent and BEAUTY_AUDIT
    are sibling repos and the relative path from bagent is unstable.
    """
    env = os.environ.get("OUTREACH_ASSETS_DIR")
    if env:
        return Path(env)
    # Conventional dev layout: bagent and BEAUTY_AUDIT share parent dir.
    candidate = Path(__file__).resolve().parent.parent.parent / "BEAUTY_AUDIT" / "docs" / "outreach-assets"
    if candidate.exists():
        return candidate
    raise RuntimeError(
        "OUTREACH_ASSETS_DIR env var not set and dev fallback not found. "
        "Set OUTREACH_ASSETS_DIR=/path/to/BEAUTY_AUDIT/docs/outreach-assets"
    )


@lru_cache(maxsize=1)
def _shell_html() -> str:
    return (_assets_dir() / "shared" / "email-shell.html").read_text(encoding="utf-8")


@lru_cache(maxsize=1)
def _footer_html() -> str:
    return (_assets_dir() / "shared" / "footer-template.html").read_text(encoding="utf-8")


# Mistune renderer with safe defaults — escape=True prevents copywriter
# from accidentally injecting <script> in MD body. We re-enable a curated
# set of inline HTML tags via a custom renderer if needed later (none
# needed for the brand voice we ship).
def _markdown_to_html(body_md: str) -> str:
    """Render markdown body to inline-HTML safe for email clients.

    We rebuild the mistune AST through a custom renderer so paragraphs,
    links, and lists pick up email-safe inline styles. Email clients
    are notoriously picky about <ul>/<ol> margins and link colors.

    Hard line breaks: copywriter writes signoffs on consecutive lines
    expecting visible line breaks. Mistune's default treats single
    newlines as whitespace (CommonMark soft break), so we enable the
    'hard_wrap' plugin → every \\n becomes <br>. Paragraph breaks
    (blank lines) still work as normal paragraphs.
    """
    renderer = _EmailHtmlRenderer()
    md = mistune.create_markdown(
        escape=True,
        renderer=renderer,
        hard_wrap=True,
    )
    return md(body_md or "")


class _EmailHtmlRenderer(mistune.HTMLRenderer):
    """Inline-styled HTML for email clients (Outlook in particular)."""

    def paragraph(self, text: str) -> str:
        return f'<p style="margin:0 0 14px 0; font-size:15px; line-height:1.65; color:#1F2937;">{text}</p>\n'

    def heading(self, text: str, level: int, **attrs: object) -> str:
        size = {1: 22, 2: 19, 3: 17, 4: 16, 5: 15, 6: 14}.get(level, 16)
        return (
            f'<h{level} style="margin:18px 0 10px 0; font-size:{size}px; '
            f'line-height:1.3; font-weight:600; color:#1F2937;">{text}</h{level}>\n'
        )

    def link(self, text: str, url: str, title: str | None = None) -> str:
        title_attr = f' title="{mistune.escape(title)}"' if title else ""
        # CTA-like text gets pill button styling; everything else inline link.
        # Mistune renders inline emphasis BEFORE invoking link(), so the
        # incoming `text` for `[**foo**](url)` is already wrapped in our
        # <strong> tag. Detect that markup, strip it, and emit pill.
        # We re-derive the inner copy by stripping the styled <strong>
        # we ourselves emit from strong(); regex-light because we control
        # both ends of the pipeline.
        stripped = text.strip()
        cta_match = re.fullmatch(
            r'<strong[^>]*>(.+)</strong>', stripped, flags=re.DOTALL
        )
        if cta_match:
            inner = cta_match.group(1).strip()
            return (
                f'<a href="{url}"{title_attr} '
                f'style="display:inline-block; background:#D4A574; color:#FFFFFF; '
                f'text-decoration:none; padding:12px 24px; border-radius:8px; '
                f'font-weight:600; font-size:15px; margin:8px 0; '
                f'mso-padding-alt:12px 24px; line-height:1.3;">{inner}</a>'
            )
        return f'<a href="{url}"{title_attr} style="color:#A0743D; text-decoration:underline;">{text}</a>'

    def emphasis(self, text: str) -> str:
        return f'<em style="font-style:italic;">{text}</em>'

    def strong(self, text: str) -> str:
        return f'<strong style="font-weight:600; color:#1F2937;">{text}</strong>'

    def list(self, text: str, ordered: bool, **attrs: object) -> str:
        tag = "ol" if ordered else "ul"
        return (
            f'<{tag} style="margin:0 0 14px 0; padding-left:22px; '
            f'font-size:15px; line-height:1.7; color:#1F2937;">{text}</{tag}>\n'
        )

    def list_item(self, text: str, **attrs: object) -> str:
        return f'<li style="margin:0 0 4px 0;">{text}</li>\n'

    def block_quote(self, text: str) -> str:
        return (
            '<blockquote style="margin:14px 0; padding:10px 16px; '
            'border-left:3px solid #D4A574; background:#FAF6EE; '
            'font-style:italic; color:#5A4A3A;">'
            f'{text}</blockquote>\n'
        )

    def thematic_break(self) -> str:
        return '<hr style="border:none; border-top:1px solid #F0E6D6; margin:18px 0;">\n'

    def codespan(self, text: str) -> str:
        return (
            f'<code style="background:#F8F4EC; padding:2px 6px; '
            f'border-radius:4px; font-size:13px; font-family:Menlo,Monaco,Consolas,monospace;">'
            f'{text}</code>'
        )


def render_email_html(
    *,
    body_md: str,
    subject: str,
    preview_text: str | None = None,
    utm_campaign: str = "outreach",
) -> str:
    """Render copywriter MD into a full email-shell HTML doc.

    {{variable}} placeholders inside body_md are preserved in the
    output (they're rendered as plain text by mistune since '{' has
    no MD meaning) and substituted later by wintact.
    """
    body_html = _markdown_to_html(body_md)

    shell = _shell_html()
    footer = _footer_html()

    # Substitute the four shell sentinels. We don't use str.format()
    # because the templates contain wintact-bound {{variable}} tokens
    # that must survive untouched.
    out = shell
    out = out.replace("{{SUBJECT}}", _safe_escape(subject))
    out = out.replace("{{PREVIEW_TEXT}}", _safe_escape(preview_text or subject))
    out = out.replace("{{BODY_HTML}}", body_html)
    out = out.replace("{{FOOTER_HTML}}", footer)

    # The header brand-link in the shell uses {{utm_campaign}} as a
    # placeholder for static substitution at render time (not at send
    # time), because utm_campaign is template-level metadata, not
    # contact-level. We replace it now.
    out = out.replace("{{utm_campaign}}", _safe_escape(utm_campaign))

    return out


def _safe_escape(s: str) -> str:
    """Minimal HTML-escape for plain-text values going into HTML attrs."""
    if not s:
        return ""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )


def render_text_fallback(body_md: str) -> str:
    """Strip markdown to a plain-text body for text/plain MIME part.

    Wintact composes both text/html and text/plain when both bodies
    are provided. The text version improves deliverability and is
    used by clients with images blocked.
    """
    text = body_md or ""
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1: \2", text)
    text = re.sub(r"^#+\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^>\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n"
