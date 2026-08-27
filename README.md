# kotobase-engine

Database engine facade over `kotobase-peer` internals. Applications use the
provider-neutral **`kotobase.datomic.client`** namespace as a drop-in for
published `datomic.client.api` (same arg-maps and return shapes), or the
lower-level `kotobase.datomic` grammar facade. Neither depends on the
historical “peer” naming, XRPC, or a storage provider SDK.

The `:kotobase` server type is a real HTTP client for
`https://datomic.kotobase.net/api/*`. It carries immutable database values
(`db-name`, graph, basis, as-of/since/history filters) as ordinary EDN while
the edge re-derives the graph from the authenticated tenant. JVM Clojure uses
`java.net.http.HttpClient`; tests and other hosts can inject `:request-fn`.

```clojure
(require '[kotobase.datomic.client :as d])

(def client (d/client {:server-type :kotobase
                       :endpoint "https://datomic.kotobase.net"
                       :token #(System/getenv "KOTOBASE_TOKEN")}))
(def conn (d/connect client {:db-name "production"}))
(def snapshot (d/as-of (d/db conn) 42))
(d/q '[:find ?e :where [?e :person/name _]] snapshot)
```

This is published-API shape compatibility, not Cognitect proprietary wire
compatibility. A stock `com.datomic/client-cloud` binary cannot be redirected
to this endpoint.

The Datomic-shaped surface includes immutable `db` values, `transact`, `with`,
`q`, `pull`/`pull-many`, `entity`/`touch`, all four `datoms` index orders,
`seek-datoms`, `index-range`, `entid`/`ident`, `basis-t`, `as-of`, `since`, and
`history`. `at-cid` pins an immutable value to an exact content-addressed
commit; reads verify its blocks and never fall back to the mutable head.
`tx-range` exposes committed transaction reports and `listen` /
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
