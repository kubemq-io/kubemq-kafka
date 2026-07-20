# Security: SASL, TLS & Authorization

This guide covers authentication (SASL/PLAIN + SASL/SCRAM, runnable; TLS/mTLS, doc-only),
authorization (Kafka ACLs enforced via Casbin), and the two security gotchas. See
[../reference/capabilities.md](../reference/capabilities.md) for the exact ✅ / 🟡 surface.

## Posture: none by default

The runnable examples default to **no SASL** — a stock dev broker with authentication off — so they
clone-and-run without a credential store. Only the `security/sasl-plain-scram` variant needs
credentials (against a broker configured with a Kafka credential store).

## SASL (✅ Full — runnable)

The connector supports three ✅ Full SASL mechanisms:

| Mechanism | `sasl.mechanism` |
|-----------|------------------|
| SASL/PLAIN | `PLAIN` |
| SASL/SCRAM-SHA-256 | `SCRAM-SHA-256` |
| SASL/SCRAM-SHA-512 | `SCRAM-SHA-512` |

A client sets `security.protocol=SASL_PLAINTEXT` (or `SASL_SSL` over TLS), the mechanism, and its
username/password. The authenticated principal becomes the identity used for authorization. The
`security/sasl-plain-scram` example proves an authenticated produce/consume and that an unauthorized
principal is denied with `TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED`.

> **GSSAPI/Kerberos SASL is ⛔ a non-goal**; **OAUTHBEARER is ✅ SUPPORTED (SASL_SSL / OIDC only)** —
> OIDC-federated, advertised and accepted on the TLS listener only, refused on plaintext with
> `UNSUPPORTED_SASL_MECHANISM(33)`; **delegation tokens are 🔴 not-yet** (deferred). Use PLAIN or
> SCRAM (plaintext or TLS), or OAUTHBEARER over SASL_SSL. See
> [../reference/capabilities.md](../reference/capabilities.md).

## TLS & mTLS (doc-only)

> **Documentation-only.** This repo ships **no runnable TLS/mTLS example**. TLS requires broker
> certificates not present on a stock dev broker, so — like the AMQP 1.0 connector repo — the TLS
> path is documented here as the production hardening path, not a clone-and-run variant. Supply your
> own certificates and configure the KubeMQ server's shared `Security` block to exercise it.

- **TLS** is served on `:9093` (`CONNECTORS_KAFKA_TLS_PORT`). Point a client at `:9093` with
  `security.protocol=SSL` (or `SASL_SSL` to combine TLS transport with a SASL identity).
- The certificate material, CA, and mode come from the KubeMQ server's **shared `Security` block** —
  there is **no Kafka-specific certificate option** beyond the TLS port.
- **mTLS principal** — with mutual TLS, the connector derives the principal from the **CN of the
  verified client-certificate chain**. That CN-derived principal is then used for Casbin
  authorization exactly as a SASL principal would be. Provide a client certificate whose CN is the
  identity you want authorized.

> **Gotcha #2 — the TLS SAN must cover `AdvertisedHost`.** The connector advertises a single
> endpoint at `AdvertisedHost:AdvertisedPort`; a TLS client dials that advertised host, so the
> server certificate's SAN must include `AdvertisedHost`, or the client connects and then fails/hangs
> on the second round-trip. Set `CONNECTORS_KAFKA_ADVERTISED_HOST` and issue the cert accordingly.
> See [../configuration.md](../configuration.md) and [../getting-started.md](../getting-started.md).

## Authorization: Kafka ACLs → Casbin

**Enforcement is ✅ Full.** Kafka ACLs are enforced through the KubeMQ server's **Casbin** policy:
per-topic and per-group `write` / `read` checks run on every relevant RPC. An unauthorized operation
returns the standard Kafka authorization error (`TOPIC_AUTHORIZATION_FAILED` /
`GROUP_AUTHORIZATION_FAILED`).

**ACL management is 🟡 Partial.** The ACL-**management** RPCs (`DescribeAcls` / `CreateAcls` /
`DeleteAcls`, keys 29 / 30 / 31) return an honest empty view or `SECURITY_DISABLED` rather than a
full ACL store — manage authorization through the KubeMQ server's Casbin policy, not the Kafka
ACL-management RPCs. See [admin-and-topics.md](admin-and-topics.md).

> **Gotcha #8 — txn offset-commit requires Group WRITE.** Committing consumed offsets **inside a
> transaction** (`TxnOffsetCommit`) requires **WRITE** on the group — stricter than stock Kafka,
> which requires only READ (decision D141). Grant the transactional consumer WRITE on its group. See
> [transactions-eos.md](transactions-eos.md).

## Quotas

Per-principal produce + fetch quotas are a token-bucket baseline (`{Produce,Fetch}ByteRate`, `0` =
unlimited). See [../configuration.md](../configuration.md).

## Production checklist

| Goal | Configuration |
|------|---------------|
| Authenticate clients with username/password | SASL/PLAIN or SASL/SCRAM-SHA-256/512 + a broker credential store |
| Encrypt transport | `Security` block (TLS) + client `security.protocol=SSL` on `:9093` (SAN covers `AdvertisedHost`) |
| Authenticate clients with certificates (no password) | `Security` block (mTLS) + `security.protocol=SSL` + client cert (CN → principal) |
| Combine encryption + SASL identity | `security.protocol=SASL_SSL` on `:9093` |
| Authorize per topic/group | KubeMQ server Casbin policy (Kafka ACLs are enforced via Casbin) |
| Keep plain for local/dev | `security.protocol=PLAINTEXT` on `:9092`; the examples use this |

## Examples

| Variant | Family | What it shows |
|---------|--------|---------------|
| `security/sasl-plain-scram` | security | SASL/PLAIN + SCRAM-256/512 (runnable) authenticated produce/consume; denied → `*_AUTHORIZATION_FAILED`; TLS/mTLS documented (doc-only) |

## See Also

- [../configuration.md](../configuration.md) — the TLS port and the `Security` block.
- [admin-and-topics.md](admin-and-topics.md) — ACL management (🟡) and quotas.
- [transactions-eos.md](transactions-eos.md) — the Group-WRITE requirement for txn offset-commit.
- [../reference/error-codes.md](../reference/error-codes.md) — the authorization error codes.

## Source Code

`connectors/kafka/` SASL/SCRAM (`sasl_test.go`, `scram_test.go`, `scram_conn_test.go`), authorization
(`acl_test.go`, `authz_test.go`, `authz_conn_test.go`, `authz_txn_test.go`,
`security_negative_test.go`).
