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
`commit-at!` appends against an exact basis without reading or publishing a
mutable ref, so concurrent writers form explicit branches instead of silently
rebasing.
`merge-at!` deterministically merges such branches into one multi-parent
commit (kotobase ADR-2610101200, rule in `kotobase-engine-contract`'s
`kotobase.engine.merge`); `merge!` does the same with the current ref head and
advances the ref through the provider's compare-and-set. See
[Multi-parent merge](#multi-parent-merge).
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

## Multi-parent merge

Two writers that `commit-at!` on the same basis offline produce sibling
commits. After exchanging blocks, either writer runs

```clojure
(engine/merge-at! conn [left right] {:schema {":doc/status" {:db/cardinality :db.cardinality/one}}
                                     :database-id "tenant/db"})
```

and both get the same merge commit (argument order and duplicates do not
matter). `branch-heads` reduces a set of known commit CIDs to the siblings a
merge must name; `merge!` merges them with the ref head and publishes through
CAS.

- **Logical identity.** Each physical chain commit is projected, without new
  bytes, onto a `kotobase.logical-commit/v1` commit (one transaction over its
  `prev`); a `fold!` commit is logically its `prev`. The merge itself is the
  contract's `kotobase.logical-commit/v2` commit, recomputed (never trusted)
  whenever it is read. `logical-projection` returns the contract graph for
  `kotobase.engine.merge/state`/`verify-merge` and frontier proofs.
- **Physical commit.** chain `prev` = first canonical parent, chain `causes` =
  the others; the state is a fresh indexed snapshot of the merged Datoms, so
  `q`/`pull`/`datoms`/`entity` read through the unchanged peer path. Commits
  without `causes` keep byte-identical CIDs.
- **Reading through a merge.** `verify-at` recomputes every merge in the
  ancestry and checks the physical state against the contract state; call it
  before trusting a merge you did not compute.
- **Settings are identity.** `:schema` (keyed by stored attribute strings),
  `:database-id`, `:model-contract-root` and `:admission-policy-root` are part
  of every logical commit; converging writers must agree on them.
  `:max-closure`/`:max-work` bound the walk and replay and fail closed.
- **Limits.** `tx-range`/`since`/`history`/`as-of` follow the `prev`
  (first-parent) lineage; the full cross-branch history is the logical graph.
  The physical merge CID equals across writers only with deterministic
  `encrypt-fn`/`blind-fn`; the logical root always does.
