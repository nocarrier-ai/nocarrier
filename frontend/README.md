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
| `GET /auth/github/callback` | public    | Finishes sign-in and sends the player to play |
| `DELETE /log-out`           | public    | Ends the session                              |
| `GET /play`                 | signed in | The game (placeholder)                        |

Sign-in is handled by [Ueberauth](https://hexdocs.pm/ueberauth). The signed-in
player lives in the session cookie as a `NoCarrier.Players.Player`; there is no
database. Every request gets a `current_scope` assign, `nil` when nobody is
signed in.

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
(`NoCarrier.HordeSupervisor`). Nothing is wired to Horde yet. Node names come
from `rel/env.sh.eex`, which sets `RELEASE_NODE` to `<app>@<private IPv6>`
whenever `FLY_APP_NAME` is present.

`fly.toml` names the app `no-carrier`, keeps machines from auto-stopping (a
cluster needs its nodes up), and expects two of them.

```sh
fly launch --no-deploy        # keep the existing fly.toml and Dockerfile
fly secrets set \
  SECRET_KEY_BASE="$(mix phx.gen.secret)" \
  RELEASE_COOKIE="$(openssl rand -hex 32)" \
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
