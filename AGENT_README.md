Cleanlight Backend — Agent Troubleshooting Guide

Audience: Any AI agent or human engineer dropped into this repo.
Purpose: Make you productive instantly without retraining.
Style: Modular, dynamic, LAW-driven.

🧵 Methodology

LAW First

Handlers = thin shims.

smesvc = authoritative logic.

Schema = describes only what exists.

No inventions. If something is missing → stop, report.

Bootstrap by Hints

Always start with POST /hint to learn shapes, row counts, and recommendations.

Then use small /query probes with limit and (for chunks) chunk_text_max.

Fail Soft, Not Loud

Handlers must return (data, error, meta) and never raise.

If you hit a code bug, patch that module only; don’t sprawl changes.

🗂️ Repo Map
Entrypoint

Cleanlight_bk.py → Flask app. Routes /query, /hint, /schema, /openapi.json.

Unpacks (data, error, meta) tuples from handlers.

If handler raises, you’ll see HTML 500 (bad). Must patch to return JSON error instead.

Handlers (thin)

handlers/query.py → Receives body, extracts table, calls into Supabase via smesvc.

handlers/hint.py → Calls smesvc.hints.build_hints.

handlers/read_all|read_rows|write|update|delete.py → Legacy CRUD wrappers (rarely used).
LAW: Handlers never contain logic, just glue + error wrapping.

Service Layer (authoritative)

smesvc/hints.py → Builds the “hint envelope”: capabilities, coverage, recommendations.

Recommendations must reflect live contract: no select, prefer filters_str.

smesvc/query.py (if present) → Lower-level query helpers.

Other smesvc/* modules: implement actual work.

Schema (docs)

schema/base.py → Base OpenAPI info (title, version, components).

schema/paths_*.py → Each path’s spec.

schema/build.py → Assembles final OpenAPI spec.

⚖️ LAW (Non-negotiable)

Handlers: must return (data, error, meta); never raise.

smesvc: all business logic here.

Schema: only describes what exists. No ghost functions.

CI: should fail if handler calls a non-existent function.

Agents: no invention. If missing, report instead.

Query contract (only allowed keys):

✅ filters_str

✅ limit

✅ chunk_text_max (for chunks)

❌ select

❌ filters dict

❌ q (unstable; removed from contract)

🚦 Troubleshooting Workflow

When something fails:

Check contract

Did you pass only allowed keys? (see above).

If filters or select → convert to filters_str.

Check handler

Does it accept the args Cleanlight_bk.py sends?

If signature mismatch → wrap in a shim (as we did for query.handle).

Check result extraction

Don’t assume .get(). Use safe extractor:

def _rows_from_res(res):
    return getattr(res, "data", []) or []


Check Supabase client version

If you see http_client errors → version mismatch.

Fix by using supabase.create_client only; don’t init SyncPostgrestClient directly.

Check /hint recommendations

Must reflect working /query calls. Update if drifted.

🔍 What’s Missing / Needs Improvement

Consistency: Gateway should accept both filters and filters_str. Right now only filters_str works.

Error Handling: Some paths still return HTML 500s; all handlers should wrap and return JSON error.

Edges Extraction: Must use safe _rows_from_res, not res.get().

Graph search: q path is unstable → banned. Always use filters_str.

Hints Output: Trimmed now, but should always emit valid example calls. No drift.

Tests: Add CI check that runs /hint and /query with minimal payloads; fails if schema and reality diverge.

🎯 Goal

Modular → each module does one thing; easy to patch in isolation.

Dynamic → schema and hints are generated live, not hard-coded.

Self-aware → agents bootstrap from /hint + /query, not from stale docs.

Stable → LAW prevents drift and invention.
