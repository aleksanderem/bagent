"""Seed full demo outreach funnel in prod Supabase for the approval queue.

Run from bagent root:
    uv run python scripts/seed_outreach_demo.py

Idempotent — re-running deletes prior demo rows before re-inserting.
Cleanup later:
    DELETE FROM outreach_audience_segments WHERE name LIKE 'demo_%';
    DELETE FROM outreach_template_variants WHERE created_by = 'demo_seed';
    DELETE FROM outreach_sequences         WHERE name LIKE 'demo_%';

What gets inserted:
    1. One audience segment (scale cohort, Warsaw beauty mass)
    2. Five email templates aligned with the 5 sequence steps:
       d0_cold_desc_gap, d3_followup, d7_case_study, d7_last_chance,
       d14_final_offer (all with rendered body_html)
    3. One sequence with DAG referencing all five templates by their
       step_key (so the orchestrator can resolve them at send time and
       the visual diagram in /admin/outreach/approval-queue links each
       step node to its real template).

Marker for demo cleanup: created_by='demo_seed' on every template
inserted here, so production rows from the real subagent team aren't
mistakenly purged.
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # picks up bagent/.env

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.sb_client import make_supabase_client
from pipelines.outreach_loader import parse_template_md
from services.email_renderer import render_email_html

sb = make_supabase_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

REPO = Path(__file__).resolve().parent.parent.parent / "BEAUTY_AUDIT"
TEMPLATES_DIR = REPO / "docs" / "outreach-assets" / "funnels" / "audit" / "templates"

DEMO_TEMPLATE_FILES = [
    "d0_cold_desc_gap_beauty_mass_a.md",
    "d3_followup_beauty_mass_a.md",
    "d7_case_study_beauty_mass_a.md",
    "d7_last_chance_beauty_mass_a.md",
    "d14_final_offer_beauty_mass_a.md",
]


def cleanup_prior_demo_rows() -> None:
    """Idempotent — clean any rows from a previous seed run."""
    sb.table("outreach_audience_segments").delete().like("name", "demo_%").execute()
    sb.table("outreach_template_variants").delete().eq("created_by", "demo_seed").execute()
    sb.table("outreach_sequences").delete().like("name", "demo_%").execute()


def seed_segment() -> None:
    sb.table("outreach_audience_segments").insert({
        "funnel": "audit",
        "name": "demo_scale_cold_desc_gap_warszawa",
        "description": (
            "Salony kosmetyczne w Warszawie z >=40% luką w opisach usług, "
            "primary_category_id w (manicure, kosmetyczka), z lead-score >= 6.0, "
            "niesubskrybowani, bez kontaktu w ostatnich 90 dniach. "
            "Estimated size: 312 kontaktów."
        ),
        "cohort": "scale",
        "filter_json": {
            "and": [
                {"salon.salon_city": "Warszawa"},
                {"diff.desc_gap_pct": [">=", 40]},
                {"salon.primary_category_id": ["IN", ["manicure", "kosmetyczka"]]},
            ]
        },
        "estimated_size": 312,
        "approval_status": "pending_review",
        "compliance_status": "compliant",
        "compliance_reviewed_by": "compliance-pl-reviewer",
        "created_by": "salon-segmentation-architect",
    }).execute()


def seed_templates() -> None:
    """Insert all 5 templates rendered through the brand shell."""
    for filename in DEMO_TEMPLATE_FILES:
        src = TEMPLATES_DIR / filename
        front, subject, body = parse_template_md(src)
        utm_campaign = f"{front['funnel']}_{front['step_key']}"
        body_html = render_email_html(
            body_md=body,
            subject=subject,
            preview_text=front.get("preview_text"),
            utm_campaign=utm_campaign,
        )
        sb.table("outreach_template_variants").insert({
            "funnel": front["funnel"],
            "step_key": front["step_key"],
            "signal_type": front.get("signal_type"),
            "vertical": front["vertical"],
            "subject": subject,
            "body_md": body,
            "body_html": body_html,
            "preview_text": front.get("preview_text"),
            "variables_required": front.get("variables_required") or [],
            "variant_label": front.get("variant_label", "A"),
            "approval_status": "pending_review",
            "compliance_status": "compliant",
            "compliance_reviewed_by": "compliance-pl-reviewer",
            "created_by": "demo_seed",
        }).execute()


def seed_sequence() -> None:
    """One DAG referencing all 5 templates by their step_key."""
    sb.table("outreach_sequences").insert({
        "funnel": "audit",
        "name": "demo_cold_to_audit_buyer",
        "description": (
            "DAG cold->buyer: D0 cold opener (desc_gap signal) -> "
            "D3 follow-up jeśli unopened -> D7 case study jeśli engaged + "
            "D7 last-chance jeśli dormant -> D14 final. "
            "5 kroków, 3 branche, exit conditions: "
            "audit_purchased / unsubscribed / 3_consecutive_unopened."
        ),
        "entry_state": "cold",
        "flow_json": {
            "steps": [
                {
                    "key": "d0",
                    "template_step_key": "d0_cold_desc_gap",
                    "delay_hours_after_entry": 0,
                },
                {
                    "key": "d3_followup",
                    "template_step_key": "d3_followup",
                    "delay_hours_after_entry": 72,
                    "predicate": {"and": [{"prev_message.opened_at": "is_null"}]},
                },
                {
                    "key": "d7_engaged",
                    "template_step_key": "d7_case_study",
                    "delay_hours_after_entry": 168,
                    "predicate": {"prev_message.opened_at": "not_null"},
                },
                {
                    "key": "d7_dormant",
                    "template_step_key": "d7_last_chance",
                    "delay_hours_after_entry": 168,
                    "predicate": {"prev_message.opened_at": "is_null"},
                },
                {
                    "key": "d14_final",
                    "template_step_key": "d14_final_offer",
                    "delay_hours_after_entry": 336,
                },
            ],
            "exit_conditions": [
                {"event": "audit_purchased", "transition_to_state": "audit_buyer"},
                {"event": "unsubscribed", "transition_to_state": "opted_out"},
                {"event": "3_consecutive_unopened", "transition_to_state": "cold_lapsed"},
            ],
        },
        "active": False,
        "approval_status": "pending_review",
        "compliance_status": "compliant",
        "compliance_reviewed_by": "compliance-pl-reviewer",
        "created_by": "outreach-sequence-designer",
    }).execute()


def main() -> None:
    cleanup_prior_demo_rows()
    seed_segment()
    seed_templates()
    seed_sequence()
    print(
        "Seeded demo data:\n"
        "  • 1 segment (scale cohort, Warsaw beauty mass)\n"
        f"  • {len(DEMO_TEMPLATE_FILES)} templates (D0 / D3 / D7 case study / D7 last chance / D14)\n"
        "  • 1 sequence (DAG with 5 steps, 3 branches, 3 exit conditions)"
    )


if __name__ == "__main__":
    main()
