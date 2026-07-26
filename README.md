# kotobase-engine

Database engine facade over `kotobase-peer` internals. Applications see
`transact!`, `datoms`, `q`, `query`, and `pull`; they do not depend on the
historical “peer” naming or on a storage provider SDK.

The JVM facade is synchronous. The ClojureScript facade returns Promises and
trampolines synchronous IPLD traversal over asynchronous S3/R2/IPFS reads.
It awaits every immutable block write before publishing the mutable database
ref.
