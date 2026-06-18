# matrix-soulseek-bridge

A chat bridge between a [Soulseek](https://www.slsknet.org/) chat room and a
[Matrix](https://matrix.org/) room, written in Go.

- **Soulseek → Matrix:** each Soulseek user appears in the Matrix room as a
  dedicated "ghost" user (via a Matrix [application service](https://spec.matrix.org/latest/application-service-api/)),
  so messages look native and are attributed to the right person.
- **Matrix → Soulseek:** messages from real Matrix users are echoed into the
  Soulseek room by the bridge's Soulseek account, prefixed with `[M] Name:`.
- **Unsupported content:** anything Matrix supports but Soulseek cannot carry
  (images, video, audio, files, locations) is rendered on the Soulseek side as a
  human-readable placeholder, e.g. `[M] Alice sent an image (cat.png)`.

## How it works

```
                 ghost users (@soulseek_*)            [M] Name: ...
  Matrix room  <───────────────────────── bridge ─────────────────────►  Soulseek room
               ─────────────────────────►        ◄─────────────────────
                 m.room.message events              SayChatroom messages
```

- The Matrix side uses [`mautrix-go`](https://github.com/mautrix/mautrix-go)'s
  `appservice` package.
- The Soulseek side is a thin client built on
  [`bh90210/soul`](https://github.com/bh90210/soul), which provides the
  Soulseek protocol message (de)serialization.

Loops are prevented in both directions: Soulseek messages from the bridge's own
account are ignored, and Matrix events from appservice-owned users (ghosts and
the bot) are ignored.

## Project layout

| Path | Purpose |
| --- | --- |
| `main.go` | Entry point: loads config, validates tokens, runs the bridge. |
| `internal/config` | Loads and validates `config.yaml`. |
| `internal/soulseek` | Minimal Soulseek chat client (connect, login, join, say). |
| `internal/matrix` | Appservice wrapper: ghost users, sending, event intake. |
| `internal/bridge` | Glue wiring both sides together, with reconnection. |

## Setup

### 1. Build

```sh
go build -o matrix-soulseek-bridge .
```

### 2. Configure

Copy the sample files and fill them in. Both files are heavily commented.

```sh
cp sample.config.yaml config.yaml
cp sample.registration.yaml registration.yaml
```

Generate the shared tokens (use the same value in both files for each token):

```sh
openssl rand -hex 32   # as_token
openssl rand -hex 32   # hs_token
```

Key things to set consistently:

- `as_token` / `hs_token` must be **identical** in `config.yaml` and
  `registration.yaml` (the bridge checks this on startup).
- The `namespaces.users` regexes in `registration.yaml` must match your
  homeserver domain and cover both the ghost template
  (`appservice.username_template`) and the bot (`appservice.bot_username`).
- `matrix.room_id` is the internal room ID (starts with `!`), not an alias.

### 3. Register the appservice with your homeserver

For Synapse, add the registration file to `homeserver.yaml`:

```yaml
app_service_config_files:
  - /path/to/registration.yaml
```

Then restart Synapse.

### 4. Invite the bot and run

Invite the bridge bot (`@soulseekbot:your.domain` by default) into the target
Matrix room and give it permission to invite users (so it can pull ghosts in).
Then run:

```sh
./matrix-soulseek-bridge -config config.yaml -registration registration.yaml
```

Both flags default to `config.yaml` and `registration.yaml` in the working
directory.

## Configuration reference

See [`sample.config.yaml`](sample.config.yaml) and
[`sample.registration.yaml`](sample.registration.yaml) — every setting is
documented inline.

## Status & limitations

- Bridges a single Soulseek room to a single Matrix room.
- Media is bridged as a text placeholder only; there is no file transfer.
- The bridge connects to Soulseek with automatic reconnection (exponential
  backoff up to 60s).

## License

See [LICENSE](LICENSE) if present; otherwise no license is currently declared.
