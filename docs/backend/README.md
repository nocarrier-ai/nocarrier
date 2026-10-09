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
- **Aggregate**: the consistency boundary. Each aggregate instance owns exactly one
  subject, `<kind>.<id>`, and that subject is its consistency boundary. None is
  special: on every tick the winner fans out one command per aggregate with work,
  each resolves its own tick over its own state, its events land on its own subject,
  and projections fold them. The aggregates are:
  - **universe clock** — single instance; `UniverseCreated`, then `TickAdvanced` forever.
  - **sector** — who is present, movement, combat. One `TickResolved` per sector-tick.
  - **port** — the trading post: born on `PortCreated` with its stance, capacity
    and regen per commodity, then every trade. Each event carries the port's
    state after it, so the last event on the subject is the port.
  - **ship** — the hull: position, cargo, condition. (planned)
  - **planet** — born on `PlanetCreated` with its sector, class and starting
    colonists. Colonies and what is on the surface come later.
  - **avatar** — the fleet admiral: identity and lifecycle, with doctrine and plan on
    their own subjects.
  Cross-aggregate effects are never a second write. A trade is one event on the
  port; the ship's cargo and credits are projections of it.
- **Projection**: a fold over event streams into a read model (KV bucket). Every read
  model must be rebuildable by replay from the streams.
- **Intent**: The output of a decision model pipeline and an element of an avatar's plan. An action the avatar intends to
  take on a future tick (move, trade, build, deploy, attack, ...). Consumed and validated by the execute phase. An intent is a
  proposal, the _resolver_ applies or rejects it against sector state.
- **Plan**: an avatar's queue of intents. Revised by decisions, consumed by
  execution, and continues executing when no decision happens.
- **Doctrine**: the avatar's standing orders, updated via command. Doctrine does
  not apply instantly. Delivery latency is the relay hop count plus the final leg
  out to the ship, times whatever the player's tier gets on the relays: free tier
  rides free bandwidth and waits longer. An unreachable ship continues to run on the
  doctrine it last received and its current plan.

## Game loop

Every tick (default 60s, never smaller than 30s, fixed in the "big bang" event):

1. **Advance**: exactly one instance _wins_ the tick (see Pacing) and dispatches.
2. **Decide**: any avatars that are "due" (subscription-tier cadence, or triggers
   raised by the previous tick) get a decision pass. A decision asks the
   model typed questions (yes/no probability, choice over a legal-option
   menu, score), expands the answers into intents, and appends a full plan
   revision plus a decision record. Cadence is a function of subscription tier and
   nothing else — never of where the ship is. Free-tier avatars hold less relay
   bandwidth, so they both consult less often and wait longer for new orders; their
   plans keep executing regardless.
3. **Execute**: every aggregate with work resolves once — sectors, ports, and
   later ships and planets, each from its own command. A sector consumes the
   next intent of each local avatar's plan (or falls back to deterministic
   doctrine standing orders on an empty plan), delivers doctrine whose relay
   delay has elapsed, applies simultaneous movement and combat rules, and
   appends exactly one `TickResolved` event for the sector and tick. A port
   resolves the trades of ships present against its own stock. A plan is a
   sequence of intents consumed one per tick, so an arrival at T and a trade
   at T+1 need no ordering between aggregates: the port reads a read model
   that already folded the arrival.

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
3a. _Q == 0_ (no events at all): every sector exists from the big bang, but an
   idle one has resolved nothing, so there is no tick T-1 to wait for. It
   resolves whatever tick first gives it work, and is an ordinary sector with
   history from then on. The dispatcher sends a never-resolved sector exactly
   one tick, and a handler that dies is redelivered well inside one tick
   period, so two of its commands are only ever in flight if a resolver hangs
   for longer than a tick. Even then the guarded append admits one and the
   loser is re-dispatched.
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
              port.<sector_id>        PortCreated, then TradeCompleted (one port per sector)
              ship.<ship_id>          (planned)
              planet.<planet_id>      PlanetCreated (ids assigned at the big bang; Terra is 1)
              avatar.<avatar_id>      AdmiralCommissioned, later lifecycle
              plan.<avatar_id>        PlanRevised
              doctrine.<avatar_id>    DoctrineUpdated
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
- `port-status`     Phoenix-facing books per port; see Projections
- `leaderboards`    Phoenix-facing rankings
- `doctrine`        Phoenix-facing latest doctrine per avatar

## Projections

Each projection is one sequential durable consumer (`MaxAckPending` 1) on
`EVENTS`, filtered to the aggregate kinds it folds, so it sees a single
ordered sequence across aggregates. Every entry stores its value together with
the last applied stream sequence, or the aggregate's revision when each event
carries the aggregate's full state. A redelivered or replayed event at or below
the stored value is skipped. One consumer means one writer per entry, so a
write is a single revision-checked update; a conflict naks and the redelivery
re-reads. This is what keeps projections idempotent while events stay deltas.

Projections switch on the event type and decode into the owning aggregate's
event struct. Nothing reacts to an event by sending a command: cross-aggregate
effects are projection updates that the tick dispatcher reads.

`active-sectors` (implemented) is the dispatcher's execute-side read model. It
folds `sector.*` and `avatar.*`. Entry per sector: `{last_resolved, seq}`.

TickResolved advances
`last_resolved` and deletes the entry when pending is false. 
AdmiralCommissioned puts the new flagship's home sector in the bucket, stamping
`last_resolved` with the tick *before* the commissioning tick, so the sector
resolves forward from the commissioning tick itself. The stamp is one behind
because `last_resolved` means a tick that really did resolve on `sector.<id>`;
claiming the commissioning tick would skip it. Anchoring to the event's tick
rather than a hardcoded -1 is what keeps the dispatcher from replaying every
tick since the big bang. `max` means a sector other ships have already resolved
past never regresses.

An entry here means a sector has work, not that it exists: every sector exists
from the big bang, which fixes the map and the link routes between sectors.
Idle sectors are simply absent. This is the only activation path today, and it is what makes
commissioning start the game: with no entry here the home sector is never
dispatched an ExecuteTick and the new flagship sits in a sector that never
resolves a tick. 

A sector not in the bucket costs nothing. Rules the real resolver must honor:
pending is false only when the sector's next tick is provably a no-op
(pending must stay true while intents remain, ships with standing orders are
present, or doctrine is undelivered). 

Known TODO: when movement is coded, the
projection must also upsert the destination sector's entry for each
`ShipMoved` outcome, or ships arrive into dead sectors and stall. 

Any future event that gives an idle sector work needs the same stamp: one tick
behind the first tick the sector should resolve.

`doctrine` (implemented) folds `doctrine.*` into the `doctrine` bucket, keyed
by avatar ID. The value is the latest DoctrineUpdated itself (revision, tick,
hash, orders, hooks); an event whose revision is not newer than the stored one
is skipped. Phoenix reads it to show the current doctrine.

`avatar-status` (implemented) folds `avatar.*` into the `avatar-status` bucket,
keyed by avatar ID. Commissioning creates the entry: who the admiral is and
where the flagship started. What a player watches change — position, cargo,
condition, credits — is the ship aggregate's state, which this projection does
not fold yet.

`port-status` (implemented) folds `port.*` into the `port-status` bucket, keyed
by sector. `PortCreated` makes the entry — the terms per commodity with
everything at capacity — and every port event carries the port's state after
it, so the fold replaces the entry, guarded by `seq`.

`due-avatars` (implemented) is the dispatcher's decide-side read model. It folds
`avatar.*` and `plan.*`. Entry per avatar: `{next_tick, seq}`.
AdmiralCommissioned schedules the admiral on its commissioning tick (a new
admiral has no plan yet), and PlanRevised pushes `next_tick` out by the cadence.
Nothing records that a DecideNow was *dispatched*, only that a decision landed,
so a lost command or a failed model call leaves the admiral due and the next
tick's winner dispatches it again. That is the same straggler recovery the
execute side gets from `active-sectors`.

Known TODO: cadence is a package constant, one tick for everyone. Subscription
tiers make it per avatar (free-tier admirals hold less relay bandwidth and so
consult less often), and triggers raised inside TickResolved must be
able to pull `next_tick` forward before the cadence is up. A retired admiral has
no removal path yet, because no lifecycle event retires one.

## Services (Phoenix contract)

Phoenix (web app; also hosts telnet via ThousandIsland under its own
supervision tree) never touches event streams directly. NATS permissions
enforce this. The contract is:

- **Commands**: request/reply to the command micro service (queue group), e.g.
  cmd.doctrine.update -> validate -> append DoctrineUpdated -> reply with
  `{revision, tick, hash}`. Invalid doctrine replies 400, a lost race 409.
  The reply means accepted, not delivered; delivery is game state.
  cmd.avatar.commission -> validate -> append AdmiralCommissioned -> reply with
  `{avatar_id, ship_name, home_sector, tick}`, which starts the game
  for a player. Commission-once is the guarded append itself: expecting last
  sequence 0 on `avatar.<avatar_id>` admits exactly one commission, so there is
  no read before it. An invalid command replies 400; an admiral who already
  exists replies 409, and unlike the doctrine 409 that is permanent, not a
  "resubmit". Every admiral spawns in sector 0; a real spawn picked from the
  universe map is a TODO, as is the starting loadout, which no event carries
  until there are economy rules to set it by.
  cmd.port.trade -> validate against the port's books -> append TradeCompleted
  -> reply with `{sector_id, ship_id, commodity, units, available, tick}`.
  The port is the last event on `port.<sector_id>` — `PortCreated` at the big
  bang, then every trade, each carrying the port's state after it — and the
  append expects that sequence, so two trades racing for the same stock cannot
  both succeed. An invalid command or a sector without a port replies 400,
  more than the port can fill 422, a lost race 409 (resubmit). Regeneration
  toward capacity is a separate command the port resolves on its tick, not
  yet implemented.
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
NOCARRIER_EMBEDDED_NATS, NOCARRIER_SECTORS, NOCARRIER_TICK_PERIOD,
NOCARRIER_EXECUTE_WORKERS, NOCARRIER_DECIDE_WORKERS, NOCARRIER_JEV_URL,
NOCARRIER_HTTP_ADDR, NOCARRIER_LOG_LEVEL, and NO_COLOR for a plain-text
startup banner). 

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
             streams, clock, dispatch, sector, decide, projector, service,
             devnats, natstest (test-only)}
- `web/`       Phoenix app (LiveView + telnet), NATS via Gnat, per-node feed
             subscription manager rebroadcasting via local PubSub
- `proto/`     `api/` (shared) and `events/` (Go-only)
- `deploy/`    NATS cluster config, per-app fly.toml + Dockerfile

