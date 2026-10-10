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
(def conn (engine/open {... :merge {:database-id "tenant/db"
                                    :schema {":doc/status" {:db/cardinality :db.cardinality/one}}}}))
(engine/merge-at! conn [left right])
```

and both get the same merge commit (argument order and duplicates do not
matter). `branch-heads` reduces a set of known commit CIDs to the siblings a
merge must name. `merge!` merges them with the ref head and publishes through
CAS. Merging is implemented on the Promise facade only; the JVM entry points
throw `:kotobase.engine/unsupported-platform`.

- **Settings are identity and have no defaults.** `:database-id` and
  `:schema` are written into every logical commit, so converging writers
  must agree on them. Configure them once at `open` under `:merge` (reads
  need them too) or pass them per call. Schema keys are the *stored*
  attribute strings (`":doc/status"`). Only `:db/cardinality`
  (`:db.cardinality/one|many`) and `:db/unique` (`:db.unique/value|identity`)
  are accepted, and keyword keys or other keys are rejected (`:invalid-schema`).
  `:database-id` is a label: the contract's single-genesis rule, not the
  label, is what stops unrelated histories from merging.
- **One semantics for storage and merge.** kotobase-peer stores raw quads, so
  every attribute behaves as cardinality-many and non-unique. The merge
  contract applies the schema. These agree only when every linear
  transaction is valid for a serial executor:
  - a cardinality-one value is replaced by an explicit retract plus an
    assert;
  - a `:db/unique` value is never asserted while another entity holds it.

  The `kotobase.datomic` layer writes exactly this. Any history that breaks
  either rule is rejected with `:schema-violation` by merging, verification
  and reads through merges. This was chosen over re-interpreting raw quads,
  which would make `q` and the merge disagree about the same commit.
- **Logical identity.** Each physical chain commit maps to one logical
  commit, with no new bytes:
  - a commit that adds a transaction is a `kotobase.logical-commit/v1`
    commit over its `prev`;
  - a `fold!` commit is logically its `prev`;
  - a merge is the contract's `kotobase.logical-commit/v2` commit,
    recomputed every time the merge is projected.

  `logical-projection` returns the contract graph.
- **Physical merge commit.**
  - Parents: chain `prev` is the parent with the greatest `[seq cid]`, so
    `t` never goes backwards; chain `causes` are the other parents in CID
    order. Commits without `causes` keep byte-identical CIDs.
  - State: a fresh indexed snapshot of the merged datoms.
  - Views: the union of the parents' materialized views, re-derived over
    the merged state. Views are derived projections, so the union cannot
    change data. One view name with different specs is refused
    (`:incompatible-views`). A view removed on one branch only is kept.
- **Reads verify merges.**
  - Each connection caches CIDs whose ancestry has been checked. A read at a
    CID whose uncached ancestry contains a merge (`q`, `pull`, `datoms`,
    `entity`, `view`, ...) first recomputes every merge through the
    contract. It then requires the physical state to equal the logical
    state (`:physical-mismatch` otherwise). After that, a new head costs
    one step.
  - Without `:merge` settings such a read is refused (`:unverified-merge`).
  - Merge-free ancestry is read on the same trust as before. A forged
    `fold!` snapshot on a linear chain is outside this change.
- **The ref only moves forward.** `merge!` keeps the current head when the
  merge is logically that head (an ancestor, a pre-`fold!` commit, a forged
  look-alike). A fast-forward lands only on a commit whose physical state
  has been verified, preferring a physical descendant of the current head.
  It may land on a logical descendant that is not a physical one, for
  example a branch from before a `fold!`: folds are logically transparent,
  so requiring physical ancestry would make such a branch unmergeable.
- **Log readers do not span merges.** `tx-range`, `since`, `history` and
  `as-of` follow `prev` only. Through a merge they reject with
  `:history-spans-merge` instead of dropping the other parents'
  transactions. Cross-branch history is the logical graph.
- **One budget.** `:max-closure` bounds commits walked. `:max-work` is one
  budget for the whole call. It covers the projection (charged before
  transaction blocks are fetched), retractEntity expansion, checkpoints and
  every contract call, each of which gets only the budget still unspent.
  Merge cost is O(commits × state) because of the reference replay; the
  ADR's open gates (incremental checkpoint root, trusted snapshot start)
  are the fix.
- **Convergence.** The physical merge CID is equal across writers only with
  deterministic `encrypt-fn`/`blind-fn`. The logical root is always equal.
