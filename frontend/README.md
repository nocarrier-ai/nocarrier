# NO CARRIER frontend

The web front door for _NO CARRIER_: a public landing page, GitHub sign-in, and
the `/play` page that will become the player's view of their admiral.

Phoenix 1.8 with LiveView, Tailwind CSS 4, and daisyUI for the theme. The OTP
application is `:no_carrier` (Mix does not allow a dash in an app name) and the
root module is `NoCarrier`.

## Routes

| Path                        | Access    | What it does                                  |
| --------------------------- | --------- | --------------------------------------------- |
| `GET /`                     | public    | Landing page                                  |
| `GET /auth/github`          | public    | Starts the GitHub OAuth flow                  |
| `GET /auth/github/callback` | public    | Finishes sign-in: known players go to play, new ones to `/players/new` |
| `GET /players/new`          | signed in | First sign-in only: choose the display name   |
| `POST /players`             | signed in | Creates the player in the universe            |
| `DELETE /log-out`           | public    | Ends the session                              |
| `GET /play`                 | signed in | The game (placeholder)                        |
| `GET /settings`             | signed in | The player's dial-in code for telnet          |

The telnet relay listens beside the web app; see below.

Sign-in is handled by [Ueberauth](https://hexdocs.pm/ueberauth). The signed-in
player lives in the session cookie as a `NoCarrier.Players.Player`; there is no
database. Every request gets a `current_scope` assign, `nil` when nobody is
signed in.

## Players and the universe

Phoenix reaches the universe (`nocarrierd`) over NATS with
[Gnat](https://hexdocs.pm/gnat). It never touches event streams: it reads the
`player-status` KV bucket and sends commands by request/reply.

On every GitHub sign-in Phoenix looks the OAuth ID up in `player-status`
(`oauth.github.<uid>`, then the player ID it points at). A known player is
signed straight in. An OAuth ID the universe has never seen is sent to
`/players/new` to choose a display name, so nobody has to advertise their
GitHub login. Submitting the name sends `cmd.player.create` with a player ID
and dial-in code Phoenix generated; the reply is the player, who is then
signed in. The dial-in code is the code the telnet relay asks for and is
shown on the Settings page.

The NATS side is configured by environment:

- `NATS_URL` (default `nats://127.0.0.1:4222`; required in prod), for example
  `tls://connect.ngs.global`
- `NATS_CREDS`, optional path to a `.creds` file with the user JWT and nkey
  seed, which is how NGS and any other account-based server authenticate

`config :no_carrier, :players_service` picks the implementation:
`NoCarrier.Players.Service.Nats` everywhere except tests, which use
`NoCarrier.Players.Service.Memory`, an in-memory stand-in started per test
with `setup :start_players_service`.

Locally, run the universe with its embedded NATS server and Phoenix finds it
on the default URL:

```sh
cd backend && NOCARRIER_EMBEDDED_NATS=true go run ./cmd/nocarrierd
```

## The telnet relay

`NoCarrier.Telnet` is a [ThousandIsland](https://hexdocs.pm/thousand_island)
listener that every node runs. It is on by default and stays in the
supervision tree either way: with `TELNET=false` it starts as `:ignore`.
`TELNET_PORT` picks the port, default 23, which needs root on most systems, so
for local work put `TELNET_PORT=2323` in `frontend/.env` and
`telnet localhost 2323`.

Each connection is a `NoCarrier.Telnet.Session`. It shows the relay banner
(`NoCarrier.Telnet.Banner`: block-letter NO CARRIER and the relay box in
16-color ANSI, amber and cyan like the web theme, sent as UTF-8) with the
universe seed and tick (from `query.universe.status`), asks for the
dial-in code from the Settings page, and looks it up in `player-status`
(`dial-in.<code>`, then the player ID it points at). A wrong code hangs up. A
connected player sees the relay status (universe, tick, registered players,
dialed-in players, this relay and how many are online) and has three commands:
`?` for help, `/status` to refresh, `/quit` to hang up. Dropping the
connection hangs up too. The
server negotiates telnet echo and suppress-go-ahead and echoes input itself,
so `telnet` and BBS terminals such as SyncTERM both behave; a raw client such
as `nc` will show input twice.

A player has one telnet session across the whole cluster. Each session
registers `{:telnet, player_id}` in `NoCarrier.HordeRegistry`; when the same
code dials in anywhere else, the earlier session is told
`ANOTHER TERMINAL DIALED IN WITH YOUR CODE` and hung up, and the new one takes
the registration.

## Running locally

Create a GitHub OAuth app at <https://github.com/settings/developers> with the
callback URL `http://localhost:4000/auth/github/callback`. Put its credentials in
`frontend/.env` (ignored by git, read only in dev, and never overriding variables
already exported in your shell):

```sh
GITHUB_CLIENT_ID=...
GITHUB_CLIENT_SECRET=...
```

Then:

```sh
mix setup
mix phx.server
```

Visit <http://localhost:4000>. `mix precommit` compiles with warnings as
errors, formats, and runs the tests.

### A local cluster

Start named nodes and they find each other through the local `epmd`:

```sh
iex --sname a -S mix phx.server
PORT=4001 iex --sname b -S mix phx.server
```

`Node.list()` in either shell lists the other, and the `/play` page shows the
node it rendered on and how many peers it sees.

## Deploying to Fly.io

Clustering uses [libcluster](https://hexdocs.pm/libcluster) polling Fly's
internal DNS (`<app>.internal`) and [Horde](https://hexdocs.pm/horde) for a
cluster-wide registry (`NoCarrier.HordeRegistry`) and dynamic supervisor
(`NoCarrier.HordeSupervisor`). The telnet relay keeps its sessions in the
registry; the dynamic supervisor is unused so far. Node names come
from `rel/env.sh.eex`, which sets `RELEASE_NODE` to `<app>@<private IPv6>`
whenever `FLY_APP_NAME` is present.

`fly.toml` names the app `no-carrier`, keeps machines from auto-stopping (a
cluster needs its nodes up), and expects two of them. It also exposes the
telnet relay: the release runs as `nobody`, so the app listens on 2323 and
Fly's proxy maps public port 23 to it.

```sh
fly launch --no-deploy        # keep the existing fly.toml and Dockerfile
fly secrets set \
  SECRET_KEY_BASE="$(mix phx.gen.secret)" \
  RELEASE_COOKIE="$(openssl rand -hex 32)" \
  NATS_URL=tls://... \
  GITHUB_CLIENT_ID=... \
  GITHUB_CLIENT_SECRET=...
fly deploy
fly scale count 2
```

The GitHub OAuth app for production needs the callback URL
`https://no-carrier.fly.dev/auth/github/callback` (or whatever `PHX_HOST` is set
to in `fly.toml`).

To confirm the nodes found each other:

```sh
fly ssh console -C "/app/bin/no_carrier rpc 'IO.inspect(Node.list())'"
```

## Adding another sign-in provider

1. Add the `ueberauth_<provider>` package to `mix.exs`.
2. Add it to the `providers` list in `config/config.exs`.
3. Read its credentials from the environment in `config/runtime.exs`.
4. Link to `/auth/<provider>`.

`NoCarrierWeb.AuthController.callback/2` already handles any provider; the
player's id is `"<provider>:<uid>"`.
