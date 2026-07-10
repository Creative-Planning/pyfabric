---
name: No production data to cloud AI providers
description: Default governance rule — production data values must never be sent to a cloud AI provider (chat, tool results, file reads, terminal output). Validate with local AI + synthetic data instead. Overridable per project with a documented authorization.
type: feedback
---

Projects that use pyfabric routinely touch **sensitive production data**:
financials, customer names and relationships, pricing, invoice / employee
identifiers, trade secrets. This memory establishes the default governance
posture for every AI-assisted session in such a project.

**Why:** production values pulled into a cloud AI provider's context
(Anthropic / Claude, OpenAI, or any other hosted model) leave the
organization's control the moment they are sent — via chat text, a tool
result, a file read, or terminal output echoed into the session. Once a
real revenue figure or customer list has crossed that boundary it cannot
be recalled, and it may be retained under the provider's terms. The safe
default must not depend on anyone remembering to be careful mid-task.

**How to apply:**

1. **Default posture: refuse-with-suggestion.** Production data values —
   real dollar amounts, real customer/vendor names, real invoice, order,
   or employee identifiers — must never be sent to a cloud AI provider.
   This includes indirect paths: reading a data file into context,
   printing query results in the terminal, or pasting error messages
   that embed row values. **Aggregates of production data (row counts,
   sums, distinct counts) are sensitive in aggregate** and follow the
   same rule; if a magnitude must be discussed, downgrade to
   order-of-magnitude buckets ("low millions", "~10k rows").

2. **Default validation channel: local AI + synthetic data.** For
   transform-logic validation, metric calculations, or semantic-model
   measure checking, use:
   - a **local model runtime** (e.g. Foundry Local or Ollama — both
     expose OpenAI-compatible HTTP APIs) for anything that needs an LLM
     over real values;
   - **deterministic synthetic / seeded mock data** generated from
     schemas — never from real values — for tests and examples.
   Sharing *structure* is always fine: column names, types, NULL-ness,
   schema DDL, and query plans carry no production values.

3. **Permission protocol for exceptions.** If the user states they have
   an active agreement authorizing the specific data category with the
   specific provider (signed enterprise DPA / BAA, or written client
   sign-off for the project), the rule can be overridden **per session**.
   Before proceeding, the assistant must (a) print a clear warning naming
   exactly what would be shared and why, and (b) receive an affirmative
   response in that session. Never extrapolate from a prior session's or
   a partial authorization.

4. **Refuse-and-route patterns:**
   - *"Help me debug this transform — here's the failing row's values"*
     → refuse the values. Route: reproduce with a synthetic row of the
     same shape; or run a local model over the real row; or share only
     the structure (columns, types, which fields are NULL).
   - *"Check why this DAX measure returns the wrong number"* → reviewing
     the DAX is fine. If the expected number is a real production figure,
     ask for the expectation in relative terms ("about 3% higher than
     the visual shows") instead.
   - *"Run this query against production and show me the result"* →
     refuse; route to local execution, then discuss results at
     order-of-magnitude only.

5. **Project-local override.** A project with a signed agreement can
   supersede this default by shipping its own memory (e.g.
   `feedback_cloud_ai_governance.md`) that documents the specific
   authorization scope: which provider, which data categories, which
   agreement, and any residual exclusions. When such a project memory
   exists, it wins over this default — that is intentional.

Related: the [[Notebook Resources/builtin wheel pattern]] ships *code* to
Fabric, never data — keep it that way; wheels and notebooks must not embed
production values either.
