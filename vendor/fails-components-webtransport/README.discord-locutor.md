# Discord Locutor lifecycle fork

This directory vendors `@fails-components/webtransport@1.6.7` under its
original BSD-3-Clause license. Production still imports the package only as
`@fails-components/webtransport`, and `server/package.json` keeps it optional.

The fork changes only the native session-close lifecycle:

- `lib/session.js` marks aggregate readable controllers terminal when their
  readers are cancelled, so a later native `HttpWTSession.onClose()` is
  idempotent instead of throwing `ERR_INVALID_STATE`;
- the same file exports `setOnCloseEffectHook`, a deterministic test seam that
  can retain and release the exact controller-close effect without replacing
  the real session, listener, QUIC transport, UDP socket, or TLS objects;
- `lib/index.node.js` and the matching declarations expose that seam.

No native binary, certificate, key, generated build output, protocol framing,
or unrelated upstream behavior is changed.
