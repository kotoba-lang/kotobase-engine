# kotobase-engine

Database engine facade over `kotobase-peer` internals. Applications use the
provider-neutral `kotobase.datomic` namespace and do not depend on the
historical “peer” naming or on a storage provider SDK.

The Datomic-shaped surface includes immutable `db` values, `transact`, `with`,
`q`, `pull`/`pull-many`, `entity`/`touch`, all four `datoms` index orders,
`seek-datoms`, `index-range`, `entid`/`ident`, `basis-t`, `as-of`, `since`, and
`history`. Transactions resolve negative tempids, lookup refs and
`:db.unique/identity` upserts before publication. Cardinality, uniqueness and
value types are enforced from schema datoms. Built-in transaction functions
include `:db.fn/cas`, `:db.fn/retractAttribute`, and
`:db.fn/retractEntity`; applications can register additional pure transaction
functions in `engine/open` under `:tx-functions` and invoke them with the same
`[:function-ident arg ...]` transaction syntax.

The JVM facade is synchronous. The ClojureScript facade returns Promises and
trampolines synchronous IPLD traversal over asynchronous S3/R2/IPFS reads.
It awaits every immutable block write before publishing the mutable database
ref.
