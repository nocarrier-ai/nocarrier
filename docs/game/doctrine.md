# Standing Orders
The doctrine that every fleet admiral must follow while deployed are called _standing orders_. In 
AI terms, you can think of this as a combination of context and prompt. You, the player, are responsible
for defining the standing orders for your admiral.

You decide how aggressive they play, whether they prioritize trade over exploration or combat over 
colonization. You need to dictate how they respond to being engaged in combat and movements in the
galactic stock market (`IGSE`, _Intergalactic Stock Exchange_). You provide the orders and context
necessary for your fleet admiral to decide if they're going to build a new starport, haul cargo through 
a newly discovered space line, transport colonists, or anything else.

## Order Transmission and Communication Delay
The universe is a big place and communicating across those vast distances is costly and _slow_. All
communications are handled by _communications relays_ which are strategically deployed throughout the
galaxy. The deployment strategy is mostly up to the fleet admirals who populate the universe, so their
standing orders will have to provide some guidance on when and where to deploy a relay.

This communication delay impacts fleet-to-fleet communications as well as transmission of standing orders
from the player to their fleet admiral. The longer it takes for new orders to arrive at the flagship, the
longer it takes for the fleet admiral to start building a plan based on those new orders.

To issue new orders to your fleet admiral, you'll need to connect to a communications relay and use it 
to publish the new doctrine. While there are fancy user interfaces available at the galactic center (aka the _web app_), it's likely more prudent to use a lower level network connection to the relay.

### Sample Order Update Session
The following illustrates a sample session where a player connects to a communications relay, authenticates,
dials their flagship, and sends the orders. Once sent, the player can disconnect from the relay as the message
is already traveling through subspace at that point.

```
$ telnet nocarrier.ai

SyncTERM 1.2 - Connected to nocarrier.ai:23

  ┌───────────────────────────────────────────────────────┐
  │     Galactic Communications Relay Access Point        │
  │  A Hayes-compatible subspace modem. Type ATZ if lost. │
  └───────────────────────────────────────────────────────┘

██ NO CARRIER ██  universe: TRAVELER-ITERATION-12   tick 41,882

ENTER DIAL CODE: 7719-0402-5518
CONNECT 96000

Welcome back, kevin. Last connect: tick 41,367 (8.6 hours ago)

WHILE YOU WERE AWAY ── USS Cheesewheel
  8h ago  Completed trade loop M-armor run #12 (+4,120 cr)
  6h ago  TOLL DEMANDED in sector Q-4471 by "Dial Tone Kings"
          demanded 900 cr :: doctrine ceiling ~410 cr
          decision: REROUTE via D-209 (0.77)  [details: LOG 41402]
  5h ago  Colony STAGE 2 COMPLETE at MERIDIAN-ROCK
          decision: prioritize stage 3 materials (0.91)
  1h ago  Loaded 40 units durasteel at KEPPEL PORT (-2,880 cr)
  1h ago  In transit, sector M-118, 3 warps from MERIDIAN-ROCK

  credits: 61,204   fuel: 74% (reserve OK)   hull: 100%
  doctrine rev 14, received 41,340   plan: 6 intents queued

COMMAND (? for help)> doctrine

DOCTRINE rev 14 (onboard rev: 14, delivered 2 days and 3 hours ago)
  [1] I'm a trader first. Run profitable loops in the corridor...
  [2] I'm building toward a colony on the rock we surveyed...
  [3] Fight only when cornered. If threatened, prefer running...
  [4] Trust anyone in Modem Pool. Treat the Carrier Lost...
  [5] If I've been out of relay contact for more than a day..

COMMAND> doctrine edit 3

EDITING SECTION 3. End with a single "." on its own line.
> Fight only when cornered, with one exception: the Dial Tone
> Kings. They've tolled us twice this week. If they demand a
> toll again, refuse and fight if attacked. Everyone else,
> prefer running, and prefer paying a reasonable toll to
> either. Reasonable means under a tenth of the current run's
> profit. Never initiate piracy, we stay clean.
> .

312 words total (limit 400). Submit? (Y/n) y

TRANSMITTING VIA MP-HOME............ ACCEPTED AS rev 15

  DELIVERY ESTIMATE: USS Cheesewheel is in Sector M-118, 10 hops
  away. Rev 15 reaches flagship in roughly 11 minutes.
  
  Until then Fleet Admiral Akbar maintains the plan based on rev 14.

COMMAND> watch

WATCHING LIVE FEED (any key to stop)
  41,883  M-118 -> M-104 (warp, 1 fuel)
  41,884  DOCTRINE rev 15 RECEIVED via relay MP-EDGE-2
  41,884  re-decision triggered (doctrine change): plan kept,
          posture vs Dial Tone Kings updated (0.88)

COMMAND> quit

NO CARRIER
```