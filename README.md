# Erebus Web Runtime

`erebus-webd` is the local web traffic runtime for [Erebus](https://github.com/rafabd1/Erebus).

It captures HTTP, HTTPS, HTTP/2, and WebSocket traffic from isolated browser sessions, then turns that traffic into searchable history and an actionable site map. Requests keep their exact variants while shared bodies use content-addressed storage.

## Features

- Per-session intercepting proxy with a persistent local certificate authority
- Chromium extension capture for existing browser profiles
- Exact request variants, deduplicated bodies, and bounded storage
- Full-text search across requests, responses, headers, payloads, and WebSocket frames
- Scope profiles with host, wildcard, regular expression, and CIDR rules
- Graph projection for domains, routes, methods, parameters, and body shapes
- Request replay with explicit overrides and stable replay IDs
- Notes attached to captured requests and graph evidence
- Gap records when a capture source drops events

The service binds to loopback. Erebus starts it with a fresh process token; extension pairing issues a short-lived token tied to one active capture session.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

The extension lives in `extension/`:

```sh
cd extension
pnpm install
pnpm test
pnpm build
```

