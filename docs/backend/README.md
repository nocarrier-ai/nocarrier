# NO CARRIER Backend Design

_NO CARRIER_ is an always-running, AI-played space trading game inspired by the
mechanics and nostalgic joy of classic BBS door games. Players don't directly pilot their flagship or control their empire, their AI does.

Each player is responsible for a fleet admiral piloting a flagship. This admiral
acts on standing orders supplied by the player, resolved and carried out by an AI decision model.
The universe advances relentlessly forward through time, regardless of whether an admiral's
owner (the player) is connected.

This document describes the overall design of the backend: a single Go binary, `nocarrierd`, where
every instance runs identical code, backed by NATS JetStream as the event
store and only stateful dependency.

## Vocabulary

Strict event sourcing terms apply throughout.

- **Event**: an immutable fact appended to a stream. Events are the _only_
  persistent truth.
- **Command**: an ephemeral request handled by an aggregate or processor, which
  produces events or a rejection. Commands are never replayed and never part
  of rebuilding state. A rejected command _never_ produces an event.
- **Aggregate**: the consistency boundary. The **sector** is the main aggregate, the
  **universe clock** is a second, single-instance aggregate. Fleet admirals (avatars) have their
  own subjects for doctrine and plans. Each aggregate instance owns exactly one subject,
  `<kind>.<id>`, and that subject is its consistency boundary.
- **Projection**: a fold over event streams into a read model (KV bucket). Every read
  model must be rebuildable by replay from the streams.
- **Intent**: The output of a decision model pipeline and an element of an avatar's plan. An action the avatar intends to
  take on a future tick (move, trade, build, deploy, attack, ...). Consumed and validated by the execute phase. An intent is a
  proposal, the _resolver_ applies or rejects it against sector state.
- **Plan**: an avatar's queue of intents. Revised by decisions, consumed by
  execution, and continues executing when no decision happens.
- **Doctrine**: the avatar's standing orders, updated via command. Doctrine does
  not apply instantly. It is delivered in-game thematically through relay coverage with
  hop delay. An unreachable ship continues to run on the doctrine it last received and its current plan.

## Game loop

Every tick (default 60s, never smaller than 30s, fixed in the "big bang" event):

1. **Advance**: exactly one instance _wins_ the tick (see Pacing) and dispatches.
2. **Decide**: any avatars that are "due" (subscription-tier cadence, or triggers
   raised by the previous tick) get a decision pass. A decision asks the
   model typed questions (yes/no probability, choice over a legal-option
   menu, score), expands the answers into intents, and appends a full plan
   revision plus a decision record. Free-tier avatars decide less frequently (thematically due to slower communications speed); their
   plans keep executing regardless.
3. **Execute**: every active sector resolves once. The resolver consumes the
   next intent of each local avatar's plan (or falls back to deterministic
   doctrine standing orders on an empty plan), delivers doctrine whose relay
   delay has elapsed, applies simultaneous movement/combat/trade rules, and
   appends exactly one `TickResolved` event for the sector and tick.

The loop is self-sustaining with zero player input and zero decisions. Plans
drain, `TickResolved` keeps sectors active until nothing is pending, and idle
sectors cost nothing (no dispatch, no compute, no model calls).

## Pacing without synchronized clocks

There is no NTP-dependent scheduling and no singleton clock process. The
universe clock is a _logical clock_ aggregate: stream `CLOCK`, subject `clock.universe`, events
`UniverseCreated` (the "big bang" with tick period, seed) then `TickAdvanced(T)` forever.

Every `nocarrierd` instance runs a pacer:

- An ordered, queue grouped consumer on `CLOCK` delivers `TickAdvanced` events.
- On each event, the instance arms a local monotonic timer: one tick period
  if it won the previous tick (its ID is in the event payload), or period +
  grace (2s) + per-instance random jitter otherwise. All durations in relative `time.Duration`, with no correlation to wall clock time.
- On fire, it appends `TickAdvanced(T+1)` with
  `Nats-Expected-Last-Subject-Sequence` set to T's stream sequence.
- The stream leader's compare-and-set admits exactly one append per tick.
  Losers' appends are rejected; the winner's event re-arms everyone.
- Lost ack: an instance whose publish errors ambiguously re-reads the head. If
  the head is `TickAdvanced(T+1)` carrying its own ID, it won and proceeds.
- Do NOT put `Nats-Msg-Id` on `TickAdvanced` appends (dedupe would ack every
  racer as a "success" and multiple instances would believe they won for much split-brained drama).
- A cold start replays `CLOCK` to the head before arming any timer, and a
  driver that finds itself several ticks behind advances them one at a time
  through the same guarded append.

The winner of a tick is the dispatcher for that tick. Fundamentally, rather than having a central clock dispatch ticks, workers compete to handle the next upcoming tick, making it both cluster-safe and resilient (the tick winner dies and the next in line wins the subsequent dispatch).

## Dispatch

The winner reads two read models and fans out commands (at-least-once,
deduplicated by deterministic `Nats-Msg-Id`, duplicate window >= 3 tick
periods):

- For each entry in _active-sectors_: publish `ExecuteTick{sector_id, tick}` to
  `execute.<sector_id>` for every tick from last_resolved+1 through the current
  tick (`Msg-Id "<sector_id>@<tick>"`). Normally that's one command. After a
  stall or crash it's the catch-up range resolved strictly in order.
- For each avatar in the "due" avatars list for this tick: publish `DecideNow{avatar_id,
  tick}` to `decide.<avatar_id>` (`Msg-Id "<avatar_id>@<tick>"`).

A winner that crashes mid-fanout is repaired by the next tick's winner, whose
read models still show the unresolved work. Command streams (`EXECUTE`, `DECIDE`)
use work-queue retention, which is consumed on ack, never replayed, and not part of state.

## Execution and the single-writer guarantee

All instances share one durable pull consumer on `EXECUTE`. A handler for
`ExecuteTick{S, T}`:

1. Reads the last event on `sector.S` (tick L at stream sequence Q).
2. _L >= T_: already resolved (duplicate or redelivery). Ack, done.
3. _L < T-1_: predecessor not resolved yet; nak with delay and wait.
4. _L == T-1_: load the sector-state read model, replay any `sector.S` tail past
   its recorded sequence, run the resolver (a pure deterministic function of
   state, intents, doctrine deliveries, seed(universe seed, S, T)), and
   append `TickResolved{S, T, pending, events}` with
   `Nats-Expected-Last-Subject-Sequence`: `Q`.
5. A rejected append (wrong last sequence) means another handler resolved T
   first: ack, done. Handlers call `InProgress()` while working so live work is
   never redelivered.

The guarantee is exactly one `TickResolved` per sector per tick, enforced by
the guarded append. Everything upstream (Msg-Id dedupe, work-queue
single delivery, the L>=T check) only makes duplicate work rare and harmless. Because the resolver is deterministic, racing handlers
produce byte-identical events anyway, and any tick can be reproduced from its
recorded inputs.

`TickResolved` is an envelope: sector, tick, pending (does this sector still
have work after this tick), and the tick's domain events (`ActionExecuted`,
`ActionRejected+reason`, `ShipMoved`, `DoctrineDelivered`, `TriggerRaised`, trades,
combat, etc.). One message per sector-tick keeps the tick atomic and makes
the subject's sequence the boundary. Events carry deltas (domain facts), not
absolute state. Idempotency shows up in projections and not events.

## Decide

All instances share one durable pull consumer on `DECIDE` whose `MaxAckPending`
is the cluster-wide cap on in-flight model calls. A handler for `DecideNow`:

1. Builds perception from read models as of the commanded tick: sector
   contents via scanner quality, market/intel feed lagged by subscription
   tier plus relay hop delay, rolling memory summary, doctrine revision the
   ship has actually received.
2. Asks the decision model typed questions. The model chooses among legal
   options only (the menu is built in Go); numeric/exact data is pre-digested
   into qualitative features; other players' free text is guardrailed or reduced
   to categories.
3. Expands the chosen plan into an intent queue and appends
   `PlanRevised{avatar_id, tick, full intent queue}` to `plan.<avatar_id>` and a decision
   record (perception digest, questions, answer distributions, resulting
   plan) to `decisions.<avatar_id>`.

Model access goes through a small interface (typed questions in, typed
answers out). Initial target is Jev's hosted API but the API is an interface so the provider can be swapped.

Free-tier decisions are
budgeted hard in this handler. A model failure naks for retry, the plan
keeps executing meanwhile.

## Streams and buckets

Event streams (file storage, S2 compression where large, AllowDirect):

- `CLOCK`     clock.universe          UniverseCreated, TickAdvanced
- `EVENTS`    sector.<sector_id>      one TickResolved per sector-tick
              avatar.<avatar_id>      avatar lifecycle; DoctrineUpdated until it
                                      moves to doctrine.<avatar_id>
              plan.<avatar_id>        PlanRevised
- `DECISIONS` decisions.<avatar_id>   decision records; MaxMsgsPerSubject caps
                                      per-player history depth

All aggregate events share `EVENTS`. The subject kind matches the Go package
that owns the aggregate, and the id is a single token (the stream only binds
`<kind>.*`, so a multi-token id has nowhere to land). Guarded appends use
`Nats-Expected-Last-Subject-Sequence` against the aggregate's one subject.
`CLOCK` stays separate because nothing projects from it; `DECISIONS` is a
capped log, not aggregate events.

Command streams (work-queue retention, duplicate window 3 tick periods):

- `EXECUTE`   execute.<sector_id>     ExecuteTick
- `DECIDE`    decide.<avatar_id>      DecideNow

KV buckets (all rebuildable by replay):

- `sector-state`    resolver's latest state per sector + last applied sequence
- `active-sectors`  dispatcher input; see Projections
- `due-avatars`     dispatcher input: cadence + triggers per tick
- `avatar-status`   Phoenix-facing current state
- `leaderboards`    Phoenix-facing rankings

## Projections

Each projection is one sequential durable consumer (`MaxAckPending` 1) on
`EVENTS`, filtered to the aggregate kinds it folds, so it sees a single
ordered sequence across aggregates. Every entry stores its value together with
the last applied stream sequence. A redelivered or replayed event at or below
the stored sequence is skipped. One consumer means one writer per entry, so a
write is a single revision-checked update; a conflict naks and the redelivery
re-reads. This is what keeps projections idempotent while events stay deltas.

Projections switch on the event type and decode into the owning aggregate's
event struct. Nothing reacts to an event by sending a command: cross-aggregate
effects are projection updates that the tick dispatcher reads.

`active-sectors` (implemented) is the dispatcher's read model. It folds
`sector.*` and `avatar.*`. Entry per sector: `{last_resolved, seq}`.

TickResolved advances
`last_resolved` and deletes the entry when pending is false. 

DoctrineUpdated with a sector creates/keeps the entry (activation only). A
sector not in the bucket costs nothing. Rules the real resolver must honor:
pending is false only when the sector's next tick is provably a no-op
(pending must stay true while intents remain, ships with standing orders are
present, or doctrine is undelivered). 

Known TODO: when movement is coded, the
projection must also upsert the destination sector's entry for each
`ShipMoved` outcome, or ships arrive into dead sectors and stall. 

Known edge to fix: activation creates entries at last_resolved -1, which triggers a
harmless catch-up burst from tick 0. Activation events should carry a tick
stamp (or the fold should read the CLOCK head) before real use.

## Services (Phoenix contract)

Phoenix (web app; also hosts telnet via ThousandIsland under its own
supervision tree) never touches event streams directly. NATS permissions
enforce this. The contract is:

- **Commands**: request/reply to the command micro service (queue group), e.g.
  cmd.doctrine.update -> validate -> append DoctrineUpdated -> reply with
  revision. The reply means accepted, not delivered; delivery is game state.
- **Current state**: read KV buckets directly; KV watches drive live updates.
- **History**: request/reply to the query micro service, e.g.
  query.decisions.page reads decisions.<avatar_id> by time window with an
  ordered consumer, upcasts old events, replies with API read models.
- **Live feeds**: per-avatar display-shaped messages (projector-produced).

Cross-language schemas live in proto/api (commands, read models, feeds);
internal event schemas in proto/events. Old events are upcast on read in Go
only. Wire compatibility of proto/events is guarded in CI (buf breaking,
WIRE level) plus golden-fixture decode tests per event version. Rules
versions are recorded in events; history is never replayed under new rules.

## Process anatomy

One binary, no roles, no flags; env vars only (NATS_URL, NATS_CREDS,
NOCARRIER_EMBEDDED_NATS, NOCARRIER_TICK_PERIOD, NOCARRIER_EXECUTE_WORKERS,
NOCARRIER_DECIDE_WORKERS, NOCARRIER_JEV_URL, NOCARRIER_HTTP_ADDR,
NOCARRIER_LOG_LEVEL). 

Instance ID is a random value per
process start, used only to recognize the instance's own TickAdvanced.

Supervision tree: pacer, execute pool (CPU-sized), decide pool
(latency-sized), one loop per projection, command+query services, health
endpoints (`/healthz`, `/readyz`). 

Loops restart with backoff. Repeated failure
exits the process nonzero for the platform to replace. 

Startup: connect
NATS, idempotently ensure streams/buckets, append UniverseCreated via
guarded first append if CLOCK is empty, replay CLOCK to head, then start
loops. 

Local dev runs one process with an embedded NATS server. Production
runs a 3-node NATS cluster (R3 streams) with instances as identical clones.

Any instance can die invisibly: another pacer wins the next tick, in-flight
commands redeliver, projections resume from durable cursors.

## Repo layout

Monorepo github.com/nocarrier-ai/nocarrier:

- `backend/` -  Go module (this design); cmd/nocarrierd + internal/{loop,
             streams, clock, dispatch, sector, decide, project, service,
             devnats, natstest (test-only)}
- `web/`       Phoenix app (LiveView + telnet), NATS via Gnat, per-node feed
             subscription manager rebroadcasting via local PubSub
- `proto/`     `api/` (shared) and `events/` (Go-only)
- `deploy/`    NATS cluster config, per-app fly.toml + Dockerfile

