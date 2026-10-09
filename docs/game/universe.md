# The Universe

The universe is created once, at the _big bang_, and never regenerated. It is a
graph of numbered **sectors** joined by directed **lanes**, with **ports** that
produce and consume a handful of commodities.

Generation is a pure function of a seed, run **once**. Its output is stored in
the `universe` KV bucket with `encoding/gob`, then read by every instance at
startup. The map is not recomputed per process: generation is pure so that it is
reproducible for tests and bug reports, not so that it can be repeated in
production. The seed stays in the `UniverseCreated` event, which must agree with
the stored map or an instance refuses to start.

This is a modernized TradeWars 2002 universe. The original included numbered sectors, 
a sparse mesh of space lanes, a hard cap on lanes per sector, one-way lanes as 
an in-theme feature, dead ends worth hunting for, and a protected cluster around the 
spawning zone. TW2002's profitable play was finding two _adjacent_ ports with 
complementary economies, which made distance irrelevant and was mechanical enough
that players automated it. Here the map has a trunk network, long hauls pay more 
than short ones, and the really profitable routes are ones that have to be
discovered.

## Vocabulary

Most of these words describe how the generator builds the map, not what the
map records. The stored universe holds only what no aggregate owns and never
changes: sectors, which of them are protected space, the lanes between them,
and which lanes were public at the moment of creation. Ports and planets are
aggregates: the generator rolls them beside the map, and the big bang creates
each one as the first event on its own subject. Hub, trunk, region and pocket are not labels on
anything — a player finds a pocket by its shape and learns the highway by its
traffic, and a flag saying "pocket" would hand over the thing they are meant to
discover.

- **Sector**: a node. Identified by a positive integer. Sectors have no
  coordinates, not even inside the generator. The only distance in this
  universe is hops — how many lanes lie between two sectors — and the generator
  reasons in nothing else.
- **Lane**: a directed edge between two sectors. A two-way lane is two lane
  records. A lane has no length and no capacity: this is a graph, not Euclidean
  space, so a warp is a warp and distance between sectors is a count of hops.
  Whether a lane is public is not a property of the lane, because it changes
  over time: the map records which lanes were public at the big bang, and the
  public map as it stands now is a projection seeded from that and folding
  every publication since. Every admiral sees the universe as the public map
  plus the lanes they have discovered on their own.
- **Core**: a small, densely connected, fully two-way, fully published cluster
  containing the spawn area and the special ports. Protected space.
- **Hub**: one of a small number of well-separated sectors that anchor the
  trunk.
- **Trunk**: the lanes connecting hubs to each other and to the core. Short,
  two-way, published. The highways.
- **Region**: the set of sectors nearest a given hub.
- **Spur**: a lane into a sector with no other outbound lane worth taking. Since players cannot be trapped by rule, a spur has to allow the player to return to their last sector via the only lane available.
- **Shortcut**: a lane joining two regions outside the trunk. Usually
  unpublished. The prize.
- **Pocket**: a sector reached by one one-way lane in, leaving by a one-way lane
  to somewhere else, neither published. Concealed, never a trap.
- **Published**: in the current public map, which every admiral can see and which market
  prices are computed from. Unpublished lanes work the same as published, only varying based on how admirals become aware of it.
- **Known**: whether one particular admiral has discovered a lane. Per-avatar,
  held in a read model, and never a field on the lane.
- **Lead**: an unmapped exit an admiral has inferred without knowing where it
  goes, from a sector reporting more lanes than the admiral can account for.
- **Circuit**: a closed route an admiral runs, carrying a different commodity
  on each leg. Directional, because one-way lanes mean you rarely come back the
  way you went.

## Knowledge and the public map

**Private to one admiral.** You found the lane. Nobody else knows it exists.

**Private to several.** You sold or gave away the coordinates. The buyer knows;
the market does not. This is where the information economy lives, and it is
deliberately _not_ publication — a lane can be known to a dozen admirals and
still be worth money, because prices are computed from the public map and the
public map has not moved.

**Published.** In the public map. Every admiral can see it, prices recompute
against it, and the margin secrecy was worth is gone.

The big bang decides the initial public map: core and trunk lanes published,
regional lanes mostly published, shortcuts and pocket lanes almost never.

What an admiral **knows** is per-avatar and lives in a read model. It is never a
field on the lane. **Using a lane does not publish it**. This means an individual
admiral can see you using a lane and that observation saves them the time it would take
to launch a probe. That lane doesn't automatically get published, though the viewing
admiral can sell that information to publish it later.

Knowledge spreads privately in three ways: you find a lane yourself, you buy it,
or you are seen using it. If another ship sits in the origin sector when you depart 
down an unpublished lane, that admiral now knows the lane is there. 
It gives doctrine something concrete to decide, e.g. _do not take the shortcut if there is company in the sector._

**The public map changes only when an admiral deliberately publishes.** Being
observed spreads knowledge; it never publishes. Nothing in the simulation
publishes a lane on its own.

### Published sectors, and relays

A sector is published when at least one published lane leads into it. Core
sectors are published by construction, and a pocket reached only by unpublished
lanes is an unpublished sector.

**A relay can only be deployed in a published sector.** You cannot run a
communications relay to somewhere the galaxy has no official route to.

This is the sharpest tradeoff in the design and it falls out rather than being
bolted on. Hide a colony in a pocket and it has no relay coverage: the admiral
out there is beyond contact, running on whatever doctrine it last received,
which is exactly the situation the game's fiction is built around. Publish the
lane and you get communications and lose the secret. **Concealment and
communication are opposed.**

### Going dark

The relay rule makes a submarine playstyle available. In this strategy, 
you keep your colony or your port in an unpublished pocket. It has no relay
coverage, so nothing reaches it and nothing leaves it. Then surface
periodically: fly back out to a published sector, take on whatever doctrine has
been waiting, send the accumulated reports, and dive again.

What it costs:

- **Stale orders.** Between surfacings the admiral runs on the last doctrine it
  received and its current plan. The player cannot react to anything until the ship comes
  up for air. (gets close enough to a relay)
- **No telemetry.** The "while you were away" digest a player reads at a relay
  is built from what the ship sent. A dark ship sends nothing, so the player is
  blind too, and surfacing delivers the whole backlog at once. This is the most
  BBS thing in the game: you dial in and a week of log lands on you.
- **A signature.** Surfacing happens in public space where other admirals can
  watch you arrive. Surface at the same sector on a regular rhythm and a patient
  observer learns something lives one hop from there, and a density scan
  confirms that sector has an exit nobody has mapped. The pattern works, but it
  leaks. Varying where and when you surface is the counter, and so is surfacing
  a long way from home.

The demand it puts on the player is also interesting. Doctrine written for a
ship you will not speak to for days has to be more conditional and more
self-sufficient than doctrine for a trader working the core. A "submarine"
player's standing orders look different in kind, and writing them well is the
skill this game is designed to teach and reward.

**Decision cadence doesn't depend on geography.** How often an admiral thinks
is set by subscription tier and nothing else. What distance costs you is **order
latency**: the number of ticks before new doctrine reaches the ship, a function
of how many relay hops the orders cross and how far the ship sits from the last
relay. A ship outside coverage has unbounded latency as it won't get any orders until 
it gets within range of a relay (by movement or by constructing its own).

Going dark therefore doesn't make an admiral stupider, it makes it take longer to
receive new orders.

Probes are unaffected, because a probe reports to the ship that launched it
rather than through the relay network. An admiral outside the range of a relay
can still explore perfectly fine assuming its doctrine takes this into account.

### Why publish

- **Charting bounty.** The galactic core pays for completed maps, more for lanes further
  out. This enables a cartographer playstyle, where an admiral can skip trading and
  combat and earn through pure exploration. The bounty is also the dial that sets 
  how fast the public map fills, which is the same dial as how long discovery 
  stays a living part of the game.
- **You are the destination.** Once you own a port or a colony that needs
  supplying, you want traffic. Publishing the short way in brings other
  admirals' freighters to your door.
- **Relay coverage**, per above. A route you are committed to long term may be
  worth more with communications than with secrecy.
- **Spite.** Publish a rival's lane to collapse their margin. You would only do
  it for a lane you do not profit from, so the public map grows partly through
  conflict.

### Why not

- It collapses the margin on your own route.
- It tells competitors and pirates where you operate.
- It exposes whatever you were hiding at the far end.

### Secrecy can be a strategic advantage

Permanent secrecy is not available in this game. Every lane departs from some 
sector and the lanes departing from that sector can always be detected through
probes or, if already traveled, they're visible to that admiral.

It follows that discovery is an early-to-mid game phase rather than the permanent
engine of the economy. The graph is finite and created once, so eventually every
lane is known and only publication status varies. The long game is stock
depletion, competition for routes, colony development and combat.

## Discovery

An admiral can't be expected to sweep thousands of sectors hoping to stumble on
an unmapped exit. Discovery needs a signal that says where to look, and the
signal has to arrive at range. There are 3 different ways discovery can happen,
each with its own pros and cons.

### Density scan

Scans the sectors adjacent to the one you occupy, down published lanes and
unpublished ones alike. Per sector it reports:

- the sector id
- its **total lane count**
- a coarse activity reading: something large is present, without saying what
- whether a port is there at all, since ports broadcast
- if there's a relay in an adjacent sector, you can see the lane to it, even if you've never traveled it

The lane count is the entire point. A sector that reports four lanes when you
know two of them has two unmapped exits. The feature the model actually sees is a
**lead**: _sector 412, two unmapped exits, reads quiet._ Information can thus
be fed into exploration decision models with ranking and appropriate metadata.

### Ether probe

An expendable drone launched down one specific lane, including an unmapped one.
It reports back after a delay, like everything else that travels. It returns:

- where that lane actually goes
- for each sector it crosses: lane count, activity, and any port's stances and
  stock levels
- whether it survived. A probe that never reports has told you something too.

The report from the probe, or _lead_, might contain information like: 
_The unmapped exit from 412 reaches 1879, which has a port consuming  provisions and no published route to a producer._ 
That information can justify moving a ship.

**A probe's report arriving is an event in its own right, and doctrine needs a
hook for it.** Results land several ticks after launch, long after the decision
that launched them, so the player has to be able to say in advance what to do
with whatever comes back: _if a probe finds a port paying well for provisions
and no published route to a producer, work it before anyone else finds it._

Mechanically this needs no new machinery. The arrival raises a trigger, the
trigger makes the avatar due early, and the decision pass runs with the probe's
findings in perception and the player's hook text alongside them. The only code
change is one more entry in doctrine's hook list, which is deliberately not
added until probes exist to fire it.

### Taking the lane

You arrive somewhere, and if the lane was one-way you can't simply turn around. Doctrine
chooses between probing first and jumping blind, which is a real risk axis for a
player to write standing orders about.

### What the model is given

Exact numbers are pre-digested, per the backend design. The model does not see
"six jumps, forty fuel, two hundred credits for a probe"; it sees a lead
qualified as near or far, cheap or costly, quiet or busy, against an option menu
Go has already filtered to the legal and the affordable. Go does the arithmetic
and the graph search. The model decides whether an admiral who was told to trade
first should spend three ticks chasing a rumour.

## Nobody gets stranded

**There are no traps.** An admiral can always get home. A sector that swallows a ship
destroys a player's entire investment with no recourse.

This is a rule about the whole universe, not a check the generator runs once.
There are three ways to strand someone and strong connectivity only catches the
first.

**Topology.** A sector with no way out, or a region you can enter and not leave.
The generator must prove the graph is strongly connected, and any later mechanic
that can remove a lane — a collapse, a blockade, a severed corridor — has to
preserve that or be refused.

**Fuel.** A strongly connected graph does nothing for an admiral sitting in a
portless sector with an empty reserve. Since we can't permanently trap players,
the game needs a mechanic to to deal with this. Fuel can slowly and ambiently regenerate
through solar or some other mechanism. Players can also launch a distress beacon,
which will take time for a "tow" ship to arrive and time for the ship to tow the
admiral's ship to the nearest port.

**Information.** Being in a pocket whose exit exists but which you have not found
is a strand that looks exactly like a trap from inside the cockpit. So a density
scan run from inside a sector always reports that sector's own exits. A lane can
be hidden from the far end but never from where you're sitting, so long as you have
the ability to perform density and ether scans.

## Commodities

Three, TW2002's own: **Fuel Ore**, **Organics**, **Equipment**. Colonists are
not a commodity; they move between planets and arrive in phase 3.

There are no sinks yet. Ports consume nothing themselves and ships do not burn
anything to move — TW2002 budgeted movement as turns, and Fuel Ore was not used
up until players built on planets. At this step the economy is TW2002's: ports
buy and sell, admirals haul between them. Sinks come with planets.

### Ports

The port is an aggregate. It owns its books, a trade is one event on it, and a
ship's cargo and credits are projections of that event.

Every ordinary port trades all three commodities, as TW2002's eight port classes
did. Per commodity it is created with three terms, carried on its `PortCreated`
event:

1. **Stance** — buys or sells.
2. **Capacity** — TW2002's `max`.
3. **Regen** — a fraction of capacity per tick, always toward capacity.

And one number that moves: **available** — goods on hand for a seller, demand
remaining for a buyer. Starts at capacity, every trade reduces it, every tick
regenerates it. Every port event carries the port's state after it, so the
last event on `port.<sector_id>` is the port. The idle universe is every port
at max, which is maximum opportunity; trade is what depletes it.

Price is derived, never stored. The local factor is percent-of-max: a seller
gets dearer as it drains, a buyer pays less as it fills. The distance factor is
below.

Stances are biased by tree depth from the hub, probabilistically: deep sectors
lean toward selling Fuel Ore and Organics and buying Equipment; hubs, core and
shallow sectors lean the reverse. All eight classes appear, with a gradient
underneath. The gradient makes a long haul pay; the randomness keeps it from
being two port types.

Roughly a third of sectors have a port at the big bang, weighted so hubs almost
always do and pockets rarely do. The spawn always has one. Players build more
later.

### Why distance pays

If price were purely a function of local stock, a pair of ports two jumps apart
and a pair twenty jumps apart would yield the same margin per unit, nobody would
run the long lane, and the optimal play would collapse back to TW2002 adjacency
trading with the trunk network sitting unused.

So each buying port's base price multiplier for a commodity comes from **its hop
distance to the nearest seller of that commodity, measured over the current
public map**. A port far from any source of what it needs pays well for it, by
construction.

Measuring over published lanes only is the core of the discovery economy. 
Prices reflect the distance the public map implies, so an admiral with
an unpublished shortcut is faster than the market expects and pockets the
difference.

Which means the multiplier is not a static generation output. It's derived from
the public map as it currently stands, so when a shortcut gets published the
consuming port's distance to a producer drops, its multiplier drops, and the
margin collapses. The advantage decays because the map changed, not because we
asserted it would. Keeping it current is cheap: one multi-source search per
commodity over the published lanes, recomputed when the public map changes,
which is rare.

In other words, if someone publishes a key piece of undiscovered map, it could
dramatically change prices throughout the entire universe.

## The big bang algorithm

Five passes. Each pass states the invariant it must not break.

### Pass 1 — Skeleton

The skeleton exists to guarantee that every admiral can reach every part of the
game.

1. **Core.** The first sectors by ID form a small, dense, all-two-way,
   all-published cluster holding the spawn. Protected space.
2. **Hubs and trunk.** The next sectors are hubs, joined in a loop with the core
   spliced in. A loop rather than a line means the highway has no dead end and
   there are always two ways around a blockade.
3. **Regions.** The remaining sectors are dealt to the hubs in runs. Each region
   grows a tree from its hub — the tree guarantees reachability — bounded in
   degree and depth, then gains extra lanes joining sectors a few tree hops apart
   so there is more than one way through.

_Invariant: the graph is connected and entirely two-way at the end of this
pass._

### Pass 2 — Character

This pass makes the map feel like TW2002 instead of a road atlas.

1. **Degree cap.** No sector exceeds six lanes, TW2002's cap. Every pass checks
   before adding; nothing is pruned after.
2. **One-way conversion.** Walk a subset of non-trunk, non-core lanes and make
   them one-directional. After each conversion, check that the graph is still
   strongly connected, and revert the conversion if it is not.
3. **Pockets.** Carve the prized structures deliberately: take peripheral leaf
   sectors and give them one unpublished one-way lane in from one neighbour and
   one unpublished one-way lane out to a different one. Absent from the public
   map, findable only by scanning the single neighbour that leads in — and, per
   the scanning rule, always escapable once you are inside.
_Invariant: the graph is strongly connected. Every sector can reach every other
sector._

No conversion in this pass may produce a trap. See Nobody gets stranded: that is
a rule about the universe, and it makes this invariant a single clean check
rather than a check plus an exceptions list.

### Pass 3 — Economy

1. **Stances.** Roll each port's stance, capacity and regen per commodity,
   biased by tree depth as described under Ports.
2. **Shortcuts.** Pick a seller and a buyer of the same commodity at least
   `minPairHops` apart over the public map, and add an unpublished two-way lane
   between a sector near each. These are the routes worth discovering.
3. **Prices** are not generated. They are derived at runtime from available and
   the current public map.

_Invariant: at least K seller-to-buyer routes are shorter over all lanes than
over public lanes by `shortcutGain` hops or more._

### Pass 4 — Planets

1. **Terra.** A class M planet at the spawn, the colonist source.
2. **Placement.** Scatter the rest weighted toward tree depth and heavily toward
   pockets, never in the core. Planets cluster: some placements deliberately
   land where one already is. Up to three per sector; a second is uncommon and
   a third is rare. A pocket with a planet and no port is the thing players
   hunt for.
3. **Class.** Roll one of TW2002's seven: M, K, O, L, C, H, U. What a class
   does — production per colonist, maximum population, habitability — is the
   planet aggregate's table, not the map's.
4. **Seeded colonies.** A very small number of planets start with a small
   colony. Every other colony is player-made.

Nothing else about a planet is stored. Colonists, what they produce,
stockpiles, ownership and citadels are runtime state on the planet aggregate,
which is also where Fuel Ore finally becomes a sink.

### Pass 5 — Validate, or reseed

Run every invariant. If any fails, do not repair the graph in place — derive a
new sub-seed and generate again, recording the sub-seed that finally succeeded
so the result stays reproducible. Repair logic is where subtle, rarely-executed
bugs live; regeneration is cheap and obviously correct.

Worth asserting:

- The same seed and map version produce an identical universe, byte for byte.
- The graph is strongly connected.
- No sector exceeds the lane cap.
- Hubs exist: some sectors have many lanes, and most have two or three. This is
  the test that proves the trunk structure actually emerged rather than being
  hoped for.
- From any sector, a trunk lane is within a bounded number of jumps.
- Every pocket has an exit, that exit is not the lane you came in by, and it is
  discoverable by scanning from inside the pocket.
- Enough planted pairs route through unpublished lanes.

## Determinism

Generation is a pure function of the seed, and the map is stored rather than
recomputed. Determinism still carries weight: an instance that finds a stored
map whose big bang never finished rolls the ports and planets again from the
seed, and must get the same roster; and a seed in a bug report should reproduce
the universe that caused it.

**No coordinates, no floating point, anywhere in generation.** Warp lanes
exist precisely because there is no x and y; TW2002 had none and neither does
this. Everything the generator decides — which sectors a region tree joins,
where an extra lane goes, where a pocket exits — is a choice among nodes and
edges, made with the seeded generator and bounded by hop counts. There is
nothing to measure.

**Never iterate a Go map where the result affects output.** Map iteration order
is randomised; sort the keys.

**OBSOLETE, kept to record the decision: record a map version in
`UniverseCreated` alongside the seed.** This was the plan while the map was
derived on every startup. Storing the map removes the problem entirely: a
changed generator cannot disturb a universe that already exists, because nothing
regenerates it. The map carries a generator version as provenance. The encoding
is `gob`, chosen over a bespoke format because a field added later decodes as
its zero value on a map stored before it existed, so an existing universe keeps
loading without a reader per format version.

The original reasoning, for the record: if the map is
derived rather than stored, any later change to the generator silently moves
every ship in every running universe. The generator switches on the recorded
version and old universes keep the geography they were born with. The backend
design already requires rules versions in events; this is the one that hurts
most if it is missed.

## Known TODOs

- `avatar.StartingSector` is the string `"0"`, a placeholder for a map that did
  not exist. Once sectors are real, spawn becomes a choice: a core sector,
  possibly varied per admiral so the whole player base does not share a
  starting square.
- Hub count, region size, one-way lane fraction, pocket count and planted pair
  count are all tuning parameters with no defensible values yet. They need a
  universe to look at before they can be set.
- Whether to expose sector positions for a map view in the web app. The game
  does not need them and TW2002 managed without, but a visual map is a strong
  draw and the generator already has the data.
- **Contraband** as a fifth commodity, high margin and confiscated in the core.
  Doctrine already references staying clean, which only means something once
  there is a dirty option. Deliberately deferred: it is a risk axis, and risk
  axes are only interesting after the ordinary economy works.
- The chart bounty's value, which is an input that feeds into how fast the public map fills and so how long discovery is important to decision models. Needs a live universe to calibrate against.
- What being "seen" using a lane actually requires: mere presence in the origin
  sector, or scanner quality, or a successful scan that tick.
- Probe cost, range in hops, and loss chance in hostile space.
- The order latency formula: what a relay hop costs in ticks, and what the final
  leg from the nearest relay out to the ship costs. Needs real distances before
  it can be tuned. Whether a lane
  can be in or out of coverage, and what that does to doctrine delivery mid
  transit, is undecided.
