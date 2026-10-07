# Noise Transport

Node-to-node journal sync over `Noise_IK_25519_ChaChaPoly_SHA256` on `:8443`.
Every connection is mutually authenticated, encrypted, and checked against
fleet membership before a single sync byte flows.

> Name decode: **IK**, initiator knows the responder's key, one round trip;
> **25519**, X25519 Diffie-Hellman; **ChaChaPoly**, ChaCha20-Poly1305 framing;
> **SHA256**, handshake hashing. Broken down in full below.

## Why Noise instead of TLS

TLS carries identity in X.509 certificates, which carry validity windows,
which require wall-clock agreement across the fleet. Concord nodes run
partitioned for weeks with no clock discipline, so the old transport fought
its own framework: custom verification callbacks, disabled session resumption,
and validity windows that were issued but never enforced.

Noise carries identity as a raw static key plus one CA signature over a
parcel binding that key to a node. There is no certificate envelope, so there
is no date field to ignore and no clock to consult. The credential matches
exactly what the system uses, and rotation and revocation become journal rules
instead of certificate machinery.

## The suite name, part by part

`Noise_IK_25519_ChaChaPoly_SHA256` names the full protocol, four parts:

* **IK** is the handshake pattern. The initiator knows the responder's static
  key before connecting, so the handshake finishes in one round trip with
  both sides authenticated. Every dial target arrives with a gossiped key, so
  no other pattern is needed.
* **25519** is the Diffie-Hellman function, X25519 over Curve25519. Both sides
  combine their private key with the other's public key and arrive at the
  same shared secrets, which ratchet into the session keys.
* **ChaChaPoly** is the framing cipher, ChaCha20-Poly1305. Every transport
  message is encrypted and authenticated with it under the session keys.
* **SHA256** is the handshake hash. It drives the key schedule and the running
  transcript hash that both sides compare implicitly as the handshake
  proceeds.

Note what SHA256 here is not: it is not the parcel check. The parcel uses
SHA-256 separately, as one step inside RSA signing described below, and
membership is decided by the RSA signature, never by a hash comparison. The
two uses share an algorithm and nothing else.

Any other combination would be an equally valid suite with its own name, and
our nodes would reject it on first message. Implementation varieties of Noise are large, this page describes what we have exactly.

## Key inventory

The cluster has exactly one CA keypair, created once by the operator and
shared to every node. `ca.crt` carries the CA public key and is storage only:
Concord never performs X.509 verification and never checks dates. `ca.key`
holds the CA private key, which produces one RSA signature per node over that
node's parcel. That signature is the node's membership credential.

Each node generates its own X25519 static keypair on first boot, stored as 32
raw bytes in `noise/secret.key`. The private half never leaves disk. Beside
it sits `noise/generation`, one big-endian number counting that node's key
rotations from zero. A fifth secret, the AES key in
`memberservice/secret.key`, encrypts SWIM gossip and is unrelated to Noise.
No CA material is ever generated at runtime.

## Vocabulary

* **Static key**: the node's long-term X25519 keypair. Holding the private
  half is what the handshake proves.
* **Generation**: the node's key rotation counter. First key is 0, each
  rotation adds one.
* **Parcel**: the session token `(node ID, generation, CA signature)`,
  fixed-width head plus variable-length signature tail. The parcel is the
  membership proof; the static key is the authentication proof. A session
  needs both.
* **Pin**: the first valid key recorded for a node ID, persisted as a
  `peer.keypinned` journal event and projected into the `pinnedkeys` view.
* **Prologue**: a fixed string both sides mix into the handshake hash. Any
  mismatch fails the session, which binds every connection to this exact
  protocol and blocks cross-protocol splicing.

## The parcel, exactly

```
signable message (81 B) --SHA-256 + RSA/ca.key--> CA signature (256 B)
```

Two byte layouts matter. Do not confuse them: the first is what gets signed
at boot, the second is what travels in every session.

The signable message, 81 bytes, built once per boot:

```
 0                   25                  41                  49                 81
 ├───────────────────┼───────────────────┼───────────────────┼──────────────────┤
 │ domain prefix     │ node ID           │ generation number │ static public key│
 │ 25 bytes          │ 16 bytes          │ 8 bytes, big-end. │ 32 bytes         │
 └───────────────────┴───────────────────┴───────────────────┴──────────────────┘
```

The session parcel, 280 bytes with RSA-2048, traveling in each handshake:

```
 0                   16                  24                                280
 ├───────────────────┼───────────────────┼─────────────────────────────────┤
 │ node ID           │ generation number │ CA signature                    │
 │ 16 bytes          │ 8 bytes, big-end. │ 256 bytes                       │
 └───────────────────┴───────────────────┴─────────────────────────────────┘
```

Signing happens once per boot on the node itself; verification happens on
every peer, every session:

```
Signer (boot):                        Verifier (every session, both sides):
  domain + ID + gen + pub               domain + ID + gen + handshake key
        │                                         │
   SHA-256 hash                              SHA-256 hash
        │                                         │
  RSA sign with ca.key          compare     RSA verify with ca.crt
   (= 256 sig bytes)           ───────►     (= accept / reject)
```

The two layouts are related by the signature: signing the 81-byte message
produces the 256-byte signature embedded in the 280-byte parcel.

The message itself never travels. Only its signature does, alongside the ID
and generation in the clear.

In steps:

1. **Build.** Assemble the 81 bytes: domain, node ID, generation number,
   public key.
2. **Sign.** SHA-256 the bytes, RSA-sign the digest with `ca.key`. The
   signature is over the hash, not over the message directly, because RSA
   signs fixed-size digests.
3. **Travel.** Send the parcel `(ID, generation, signature)` in the handshake
   payload. The public key is already known to the other side from gossip or
   from completing the handshake itself.
4. **Verify.** Rebuild the 81 bytes from the claimed ID, the claimed
   generation number, and the presented public key. Hash, RSA-check against
   `ca.crt`. Accept or reject.

A passing check confirms three facts at once. The presenter holds the static
private key, since only it could complete the handshake. The key is bound to
the claimed node ID at the claimed generation number, since only the CA could
sign that tuple. And the binding is fresh enough to trust, which the pin rules
below decide.

## Boot sequence

1. Ensure the static key, creating `noise/secret.key` on first boot.
2. Ensure the generation counter, creating it at 0 on first boot.
3. Sign the parcel `(node ID, generation, public key)` with `ca.key`.
4. Gossip the public key and generation via memberlist metadata.
5. Serve `:8443` and start dialing peers. The signature travels inside each
   Noise session, never in gossip.

## Gossip

Memberlist metadata carries the Noise public key (32 bytes) and generation
per node, roughly 290 bytes total against memberlist's hard 512-byte limit.
The 256-byte CA signature is excluded for exactly this reason. Gossip itself
is AES-encrypted with the cluster gossip key.

## Session flow

```
Dialer (initiator)                          Responder
     │                                            │
     │ TCP connect to :8443                       │
     ├───────────────────────────────────────────►│
     │                                            │
     │ IK msg1 + parcel(ID, gen, sig)             │
     ├───────────────────────────────────────────►│
     │   responder verifies parcel,               │
     │   then replies                             │
     │                                            │
     │ IK msg2 + parcel(ID, gen, sig)             │
     │◄───────────────────────────────────────────┤
     │                                            │
     │ dialer verifies parcel                     │
     │                                            │
     │ HTTP POST /v1/sync (AEAD frames)           │
     ├───────────────────────────────────────────►│
     │                                            │
     │ HTTP 200 + events (AEAD frames)            │
     │◄───────────────────────────────────────────┤
     │                                            │
     │ close, session keys discarded              │
```

How identity is established, precisely: the dialer encrypts its parcel in the
first handshake message, which only the true responder can read, since reading
it requires the responder's static private key. The responder decodes the
parcel, runs the CA check and the pin check, and only then answers with its
own parcel. The dialer decodes that parcel, confirms it names the dialed node
ID, and runs the same two checks. Only then does HTTP start. Neither side
sends anything readable to an unverified peer: the responder sends no reply
bytes before verifying, and the dialer sends no sync bytes before verifying.

After the handshake, sync runs as plain HTTP/1.1 over the encrypted channel:
same `POST /v1/sync`, same cursors, same idempotent apply. One
connection carries one sync; session keys are discarded on close, so recorded
traffic cannot be decrypted later from a stolen static key.

## Verification rules

Every handshake runs both checks, in both directions, in this order:

1. **CA signature.** Rebuild the parcel, hash it, verify the RSA signature.
   Failure means non-member, rejected. No clocks, no chains, no dates.
2. **Pin table.** Look up the node ID among pinned keys.
   * No pin: first sight. Record the pin in the journal, admit.
   * Higher generation than pinned: rotation. Re-pin, admit.
   * Same generation, different key: reject with both keys in the log.
     The pin sticks.
   * Lower generation: stale, reject.
   * Exact match: admit. One view read, nothing recorded.

The responder additionally requires the presented static key to equal the
bytes the CA signed, and the dialer requires the responder to prove the
dialed node ID, so cursors cannot be poisoned by a confused peer.

## Rotation procedure

1. Run `concord node rotate-key`. It deletes `noise/secret.key` and bumps
   `noise/generation` by one.
2. Restart the node. Boot generates a fresh key, re-signs, gossips the new
   generation, and peers re-pin on first contact.

Recovery from a pin alarm is always a bump: it is an intentional act, and the
higher generation supersedes every older pin fleet-wide with no coordination.

## Threat model

* A passive observer sees TCP to `:8443` and nothing else. Handshake messages
  hide static keys, payloads are encrypted, gossip is AES-sealed.
* An active attacker without keys passes neither check and never sees sync
  bytes.
* A stolen static key impersonates its node until rotation, and the pin alarm
  fires the moment the legitimate key is also heard.
* A stolen `ca.key` mints arbitrary members. Nothing contains this: every
  node holds `ca.key` by provisioning choice, so CA compromise is fleet
  compromise under any transport.
* Partial state loss (new key at an old generation with a valid signature)
  is rejected by the pin and healed by a bump, instead of silently splitting
  the fleet.
* No Byzantine outsider: the fleet is single-tenant and every node is
  operator-provisioned with the same CA and gossip key, so a stranger cannot
  join, gossip, or handshake. Faults in scope are own-node corruption, loss,
  and partition, which the deterministic merge rules absorb and the Resonance
  suite replays.

## Non-goals

* No XX handshake: every dial target arrives with a gossiped key, so the
  first-contact pattern has no trigger in this architecture.
* No certificate handling: `ca.crt` is storage for one public key, and the
  node certificate minted beside it is unused by the transport.
* Revocation beyond pinning (explicit deny lists) plugs into the verifier
  seam and is not implemented.
