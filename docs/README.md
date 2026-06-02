# Relay developer documentation

Engineering docs for working *on* Relay. Each doc answers one kind of need, so the
docs stay focused and new ones have an obvious home:

| Doc | Need it serves | Status |
|---|---|---|
| **Explanation** | "How does it all work? What talks to what?" | [`architecture.md`](architecture.md) ✅ |
| **Reference** | "What's the exact shape of X?" | [`reference/`](reference/) ✅ |
| **How-to guides** | "How do I add a command / actor / migration?" | _to be written_ |
| **Tutorials** | "Walk me through building/running my first change." | _to be written_ |
| **Client contract** | "What must a custom client honor?" | [`client-contract.md`](client-contract.md) ✅ |

## Start here

**New to the codebase?** Read [`architecture.md`](architecture.md) top to bottom
once — it maps the actors, the per-connection tasks, the fan-out hub, and how a
request flows end to end. Everything else is lookup.

**Building a custom client?** Read [`client-contract.md`](client-contract.md) — the
conventions and obligations a client must honor (the mixed event stream, dedup by
`message_id`, attachment framing, read state, resync), alongside the message catalog
in [`reference/wire-protocol.md`](reference/wire-protocol.md).

## Reference catalogs

- [`reference/wire-protocol.md`](reference/wire-protocol.md) — framing and every
  `ClientCommand` / `ServerEvent`.
- [`reference/actors-and-channels.md`](reference/actors-and-channels.md) — every
  long-lived task, the channel it owns, and the concurrency bounds.
- [`reference/data-model.md`](reference/data-model.md) — tables, keys,
  relationships, and the uuidv7 / watermark invariants.

## Conventions for adding docs

- **Keep the doc types separate.** A how-to ("add a new `ClientCommand`") shouldn't
  drift into explaining the actor model — link to `architecture.md` instead. An
  explanation shouldn't turn into step-by-step instructions.
- **Reference tracks code.** The catalogs name real files and symbols; when you
  change `model.rs`, `hub.rs`, or a migration, update the matching reference doc in
  the same change.
- **Don't duplicate the migration.** The schema's rationale lives in migration
  comments; `data-model.md` summarizes and points back.

> Operational/product notes (design rationale, security posture, roadmap) currently
> live in the top-level [`../README.md`](../README.md).
