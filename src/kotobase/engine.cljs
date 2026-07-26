(ns kotobase.engine
  "Promise-based Kotobase engine for Worker/JavaScript storage providers."
  (:require [cljs.reader :as reader]
            [kotobase-peer.core :as peer]
            [kotobase.storage.core :as storage]))

(defrecord Database
  [storage ref-name encrypt-fn decrypt-fn blind-fn visible? max-retries
   tx-functions listeners])

(defrecord Db
  [connection basis-cid mode since-t value])

(defn open
  [{:keys [storage ref-name encrypt-fn decrypt-fn blind-fn visible? max-retries
           tx-functions]
    :or {ref-name "main" max-retries 16 tx-functions {}}}]
  (storage/validate-backend! storage)
  (doseq [[control value]
          [[:encrypt-fn encrypt-fn] [:decrypt-fn decrypt-fn]
           [:blind-fn blind-fn] [:visible? visible?]]]
    (when-not (ifn? value)
      (throw (ex-info "Kotobase engine requires an explicit security control"
                      {:type :kotobase.engine/missing-control
                       :control control}))))
  (when-not (every? ifn? (vals tx-functions))
    (throw (ex-info "Every transaction function must be callable"
                    {:type :kotobase.datomic/invalid-tx-functions})))
  (->Database storage ref-name encrypt-fn decrypt-fn blind-fn visible?
              max-retries tx-functions (atom {})))

(defn head [database]
  (if (instance? Db database)
    (js/Promise.resolve (:basis-cid database))
    (-> (storage/-read-ref (:storage database) (:ref-name database))
        js/Promise.resolve
        (.then #(some-> % :cid)))))

(defn- resolve-db [database]
  (-> (js/Promise.resolve database)
      (.then
       (fn [database]
         (if (instance? Db database)
           database
           (-> (head database)
               (.then #(->Db database % :current nil nil))))))))

(defn db
  "Return a Promise of an immutable database value pinned to the current CID."
  [connection]
  (resolve-db connection))

(defn- connection [database]
  (if (instance? Db database) (:connection database) database))

(defn tx-function [database ident]
  (let [functions (:tx-functions (connection database))]
    (or (get functions ident) (get functions (str ident)))))

(defn listen!
  "Register an in-process transaction-report listener on CONNECTION."
  [database listener]
  (when-not (ifn? listener)
    (throw (ex-info "Listener must be callable"
                    {:type :kotobase.datomic/invalid-listener})))
  (let [database (connection database)
        id (.randomUUID js/crypto)]
    (swap! (:listeners database) assoc id listener)
    id))

(defn unlisten! [database listener-id]
  (let [database (connection database)
        present? (contains? @(:listeners database) listener-id)]
    (swap! (:listeners database) dissoc listener-id)
    present?))

(defn notify-listeners! [database tx-report]
  (doseq [[_ listener] @(:listeners (connection database))]
    (try
      (listener tx-report)
      (catch :default _ nil)))
  tx-report)

(defn- block-miss [cid]
  (doto (js/Error. "kotobase engine block miss")
    (aset "kotobaseBlockMiss" cid)))

(defn- missed-cid [error]
  (loop [cause error]
    (when cause
      (or (aget cause "kotobaseBlockMiss")
          (recur (.-cause cause))))))

(defn- with-blocks
  "Trampoline synchronous IPLD reads over an async provider. Persist all
  immutable writes before publishing the mutable ref."
  [backend f]
  (let [cache (atom {})
        pending (atom [])
        get-fn (fn [cid]
                 (let [key (str cid)]
                   (if (contains? @cache key)
                     (get @cache key)
                     (throw (block-miss key)))))
        put! (fn [cid bytes]
               (let [key (str cid)]
                 (swap! cache assoc key bytes)
                 (swap! pending conj
                        (-> (storage/put-block! backend key bytes)
                            js/Promise.resolve)))
               cid)
        flush! (fn []
                 (let [writes @pending]
                   (reset! pending [])
                   (-> (clj->js writes) js/Promise.all)))
        cas! (fn [name expected next]
               (-> (flush!)
                   (.then
                    (fn [_]
                      (storage/-compare-and-set-ref!
                       backend name expected next)))
                   (.then (fn [result] (:current result)))))]
    (letfn [(retry-miss [error]
              (if-let [cid (missed-cid error)]
                (-> (storage/-get-blocks backend [cid])
                    js/Promise.resolve
                    (.then
                     (fn [found]
                       (if-let [bytes (get found cid)]
                         (do (swap! cache assoc cid bytes)
                             (step))
                         (js/Promise.reject
                          (ex-info "Kotobase block is missing"
                                   {:type :kotobase.engine/missing-block
                                    :cid cid}))))))
                (js/Promise.reject error)))
            (step []
              (try
                (-> (f get-fn put! cas!)
                    js/Promise.resolve
                    (.catch retry-miss))
                (catch :default error
                  (retry-miss error))))]
      (step))))

(defn transact! [database tx-data]
  (when (instance? Db database)
    (throw (ex-info "Cannot transact against an immutable database value"
                    {:type :kotobase.datomic/immutable-db})))
  (-> (head database)
      (.then
       (fn [current]
         (with-blocks
           (:storage database)
           (fn [get-fn put! cas!]
             (peer/commit-serialized!
              put! get-fn cas! (:ref-name database) current tx-data
              (:encrypt-fn database) (:max-retries database))))))))

(defn- async-get-fn
  "Promise-returning direct block fetch, for kotobase-peer's cold-snapshot half.

  `with-blocks` bridges an async store to a SYNCHRONOUS get-fn by throwing on a
  miss and retrying. That works while the read is still on the synchronous head
  walk (`state-at`), which is a handful of small metadata blocks. It does not
  work once hydration reaches the cold snapshot: that half runs inside promise
  continuations, and a miss thrown there does not come back through the
  trampoline's `.catch`, so the read dies on the first cold block with
  `block-miss` for a block the store demonstrably has.

  kotobase-peer already provides the way out. `hydrate-chain-cached` and
  `hot-datoms` take an optional `async-get-fn` that routes the snapshot half
  through `cold-datoms-async`, fetching blocks directly and never entering the
  sync trampoline at all. Supplying it is what makes the cljs read path work
  against any real async store; it is also what stops the snapshot half paying
  the trampoline's O(N^2) block-discovery cost (kotobase-peer#21)."
  [backend]
  (fn [cid]
    (-> (storage/-get-blocks backend [(str cid)])
        js/Promise.resolve
        (.then (fn [found] (get found (str cid)))))))

(defn- at-basis [database f]
  (let [backend (:storage database)]
    (with-blocks backend (fn [get-fn _put! _cas!] (f get-fn)))))

(defn as-of
  "Return a Promise of an immutable database value at-or-before sequence T."
  [database t]
  (-> (resolve-db database)
      (.then
       (fn [{:keys [basis-cid] :as snapshot}]
         (let [database (:connection snapshot)]
           (-> (at-basis database #(peer/as-of % basis-cid t))
               (.then
                (fn [cid]
                  (assoc snapshot :basis-cid cid
                         :mode :current :since-t nil)))))))))

(defn since [database t]
  (-> (resolve-db database)
      (.then #(assoc % :mode :since :since-t t))))

(defn history [database]
  (-> (resolve-db database)
      (.then #(assoc % :mode :history :since-t nil))))

(defn basis-cid [database]
  (-> (resolve-db database) (.then :basis-cid)))

(defn basis-t [database]
  (-> (resolve-db database)
      (.then
       (fn [{:keys [basis-cid] :as snapshot}]
         (if-not basis-cid
           nil
           (at-basis (:connection snapshot)
                     #(some-> (peer/head % basis-cid) :seq)))))))

(defn tx-range
  ([database] (tx-range database nil nil))
  ([database start end]
   (-> (resolve-db database)
       (.then
        (fn [{:keys [basis-cid] :as snapshot}]
          (let [database (:connection snapshot)]
            (at-basis
             database
             #(peer/tx-range % basis-cid start end
                             (:decrypt-fn database)))))))))

(defn- db-value [database]
  (-> (resolve-db database)
      (.then
       (fn [{:keys [basis-cid mode since-t] :as snapshot}]
         (if-let [value (:value snapshot)]
           value
           (let [database (:connection snapshot)
                 backend (:storage database)
                 fetch1 (async-get-fn backend)]
             (with-blocks
               backend
               (fn [get-fn _put! _cas!]
                 (case mode
                   :since
                   (peer/since get-fn basis-cid since-t
                               (:decrypt-fn database))

                   :history
                   (peer/history get-fn basis-cid
                                 (:blind-fn database) (:decrypt-fn database))

                   ;; Both caches nil: kotobase-peer documents that as
                   ;; byte-identical to plain hydrate-chain.
                   (peer/hydrate-chain-cached
                    get-fn basis-cid (:blind-fn database)
                    (:decrypt-fn database) nil nil fetch1))))))))))

(defn with
  "Speculatively apply normalized TX-DATA to an immutable database value."
  [database tx-data]
  (-> (resolve-db database)
      (.then
       (fn [before]
         (-> (db-value before)
             (.then
              (fn [value]
                {:db-before before
                 :db-after (assoc before :value
                                  (peer/transact value tx-data))
                 :tx-data tx-data})))))))

(defn- history-row-matches? [options {:keys [e a v_edn]}]
  (let [value (reader/read-string v_edn)
        key (case (or (:index options) :eavt)
              :eavt [e a value]
              :aevt [a e value]
              :avet [a value e]
              :vaet [value a e])
        components (:components options)]
    (= (vec components) (subvec key 0 (min (count key)
                                            (count components))))))

(defn datoms
  ([database] (datoms database nil))
  ([database options]
   (-> (resolve-db database)
       (.then
        (fn [{:keys [basis-cid mode] :as snapshot}]
          (let [database (:connection snapshot)]
            (if (= :history mode)
              (let [entity (when (and (= :eavt (:index options))
                                      (seq (:components options)))
                             (first (:components options)))]
                (-> (at-basis
                     database
                     #(peer/history-datoms
                       % basis-cid entity (:visible? database)
                       (:decrypt-fn database)))
                    (.then
                     (fn [rows]
                       (cond->> rows
                         (seq (:components options))
                         (filter #(history-row-matches? options %))
                         (:limit options) (take (:limit options))
                         true vec)))))
              (-> (db-value snapshot)
                  (.then #(peer/datoms % options
                                      (:visible? database)))))))))))

(defn q [database pattern]
  (-> (db-value database)
      (.then
       (fn [value]
         (-> (resolve-db database)
             (.then #(peer/q value pattern
                             (:visible? (:connection %)))))))))

(defn query
  ([database query] (query database query []))
  ([database query inputs]
   (-> (js/Promise.all #js [(db-value database) (resolve-db database)])
       (.then
        (fn [results]
          (let [value (aget results 0)
                snapshot (aget results 1)]
            (peer/query value query
                        (:visible? (:connection snapshot)) inputs)))))))

(defn pull [database entity pattern]
  (-> (db-value database)
      (.then #(peer/pull % entity pattern))))

(defn fold!
  "Compact the database's accumulated novelty into a fresh indexed
  snapshot. Self-retrying on head-CAS contention (`kotobase-peer.core/
  fold-serialized-if-needed!`) the same way `transact!` relies on
  `commit-serialized!`'s own self-retry -- there is no outer retry loop
  here, unlike kotobase-server's `do-fold` (which relies on ITS caller's
  `run-write` retry instead; this fn is the standalone-safe equivalent for
  a caller, like kotobase-storage-d1, with no such outer loop of its own).

  `opts` (optional map): `:threshold`/`:max-novelty`/`:max-retries`
  (kotobase-peer defaults apply) and `:views` -- a map of
  `{view-name spec-or-nil}` materialized view declarations (nil removes a
  view). A non-nil `:views` forces the fold to run even when there is no
  novelty to compact, so a caller's view declaration is never silently
  dropped (see `fold-serialized-if-needed!`'s own docstring)."
  ([database] (fold! database {}))
  ([database opts]
   (-> (head database)
       (.then
        (fn [current]
          (with-blocks
            (:storage database)
            (fn [get-fn put! cas!]
              (peer/fold-serialized-if-needed!
               put! get-fn cas! (:ref-name database) current
               (:blind-fn database) (:encrypt-fn database) (:decrypt-fn database)
               opts))))))))

(defn view
  "Rows of a fold-materialized view, always fresh (the fold-time view rows
  with unfolded novelty merged on top). nil when the graph has no head yet
  or the view isn't declared (a prior `fold!` with `:views` declares it) --
  a real miss, not an ambiguous empty read, same distinction kotobase-
  server's `do-view` makes.

  Uses the synchronous `with-blocks` trampoline (matching every other read
  in this namespace before `db-value`'s async-get-fn path was added for
  datoms/q/pull) because `kotobase-peer.core/view-rows` does not yet
  accept an `async-get-fn` -- a cold/large view read pays the same O(N^2)
  block-discovery cost `do-datoms`'s docstring documents was a REAL
  production CPU-budget failure for datoms/q before that fix. Adding
  async-get-fn support to `view-rows` itself is a kotobase-peer follow-up,
  not something to route around here by half-wiring it."
  [database view-name]
  (-> (head database)
      (.then
       (fn [current]
         (if-not current
           (js/Promise.resolve nil)
           (with-blocks
             (:storage database)
             (fn [get-fn _put! _cas!]
               (peer/view-rows get-fn current view-name
                               (:visible? database) (:decrypt-fn database)))))))))

(defn pull-many [database pattern entities]
  (-> (db-value database)
      (.then
       (fn [value]
         (mapv #(peer/pull value % pattern) entities)))))

(defn entity [database entity-id]
  (-> (db-value database)
      (.then #(peer/entity % entity-id))))

(defn entid [database id]
  (-> (db-value database)
      (.then #(peer/entid % id))))

(defn ident [database id]
  (-> (db-value database)
      (.then #(peer/ident % id))))
