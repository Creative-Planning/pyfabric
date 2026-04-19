---
name: pyfabric — descriptions are required by default
description: SemanticModel and Report builders raise on save when visible objects have no description. Always supply meaningful descriptions; don't disable strict_descriptions to dodge the check.
type: feedback
---

When using `pyfabric.items.semantic_model.SemanticModel` or
`pyfabric.items.report.Report`, every **non-hidden** Table, Column,
Measure, and (for Report) the report itself must have a non-empty
`description`. Both builders default to `strict_descriptions=True` and
raise `SemanticModelError` / `ReportError` on `save_to_disk` if any
required description is missing.

**Why this exists:** descriptions surface as tooltips in the Power BI
field list, on column headers in tables, and on slicer/card visual
headers. An empty model is a poor consumer experience — analysts
hover a measure expecting "what does this count?" and get nothing.
Forcing descriptions at the builder layer is the cheapest way to
prevent that drift.

**What to do as a Claude session writing pyfabric code:**

1. **Always supply a description** when constructing `Table`, `Column`,
   `Measure`, or `Report`. Write it as a complete sentence that
   answers "what is this and how do I read it?"

   ```python
   Column(
       "report_year", "int64",
       format_string="0",
       description="Year of the report's fiscal-period folder.",
   )
   Measure(
       "Coverage %",
       expression=...,
       format_string="0.0%",
       description=(
           "End-to-end populated-cell ratio for the section. "
           "The headline number for QA readiness."
       ),
   )
   ```

2. **Don't reach for `strict_descriptions=False`** as an escape hatch
   for "I'll fill them in later." The validation is the point. If a
   column truly doesn't need a description because it's housekeeping,
   set `is_hidden=True` instead — hidden objects are exempt from the
   description requirement (and don't appear in the field list).

3. **For housekeeping/audit columns** like `gold_loaded_at`,
   `source_file_hash`, etc., use `is_hidden=True`. Same for any
   `*_pct` raw double columns when they're sources for a measure
   that's the user-facing surface.

4. **Multi-line descriptions are encouraged** for measures that need
   it. TMDL renders consecutive `///` lines as a single description
   in Power BI's hover tooltip:

   ```python
   Measure(
       "# PDFs Not Detected",
       expression=...,
       description=(
           "PDFs with at least one in-scope section the extractor "
           "failed to find at all. Worst case — the extractor produced "
           "nothing for that section. Usually a missing detection rule "
           "or layout regression."
       ),
   )
   ```

5. **Fixtures and examples should always model the right behavior.**
   When writing a snippet for the user (or a test fixture), include
   real descriptions even if it makes the snippet longer. This is the
   "pit of success" — users who copy-paste the example get a model
   that's correct out of the gate.

If you're regenerating an existing model and need to migrate, set
`strict_descriptions=False` temporarily, run, and immediately go back
and fill in the descriptions the warning logged. Don't ship the
opt-out as the steady state.
