# kotobase-engine

Database engine facade over `kotobase-peer` internals. Applications use the
provider-neutral **`kotobase.datomic.client`** namespace as a drop-in for
published `datomic.client.api` (same arg-maps and return shapes), or the
lower-level `kotobase.datomic` grammar facade. Neither depends on the
historical “peer” naming, XRPC, or a storage provider SDK.

The Datomic-shaped surface includes immutable `db` values, `transact`, `with`,
`q`, `pull`/`pull-many`, `entity`/`touch`, all four `datoms` index orders,
`seek-datoms`, `index-range`, `entid`/`ident`, `basis-t`, `as-of`, `since`, and
`history`. `tx-range` exposes committed transaction reports and `listen` /
`unlisten` provide in-process post-commit listeners. Transactions resolve
negative tempids, lookup refs and `:db.unique/identity` upserts before
publication. Cardinality, uniqueness and value types are enforced from schema
datoms, including homogeneous, heterogeneous, and composite tuple attributes.
Built-in transaction functions include `:db.fn/cas`,
`:db.fn/retractAttribute`, and `:db.fn/retractEntity`; applications can
register additional pure transaction functions in `engine/open` under
`:tx-functions` or persist a portable `:db.type/fn` value using the bounded
`kotobase/tx-ir-v1` declarative format. Both use the same
`[:function-ident arg ...]` transaction syntax. Persisted functions may emit
only Datomic transaction operations; arbitrary host-language evaluation is
deliberately unavailable.

The JVM facade is synchronous. The ClojureScript facade returns Promises and
trampolines synchronous IPLD traversal over asynchronous S3/R2/IPFS reads.
It awaits every immutable block write before publishing the mutable database
ref.
