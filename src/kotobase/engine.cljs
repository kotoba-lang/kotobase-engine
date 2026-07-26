(ns kotobase.engine
  "Promise-based Kotobase engine for Worker/JavaScript storage providers."
  (:require [kotobase-peer.core :as peer]
            [kotobase.storage.core :as storage]))

(defrecord Database
  [storage ref-name encrypt-fn decrypt-fn blind-fn visible? max-retries])

(defn open
  [{:keys [storage ref-name encrypt-fn decrypt-fn blind-fn visible? max-retries]
    :or {ref-name "main" max-retries 16}}]
  (storage/validate-backend! storage)
  (doseq [[control value]
          [[:encrypt-fn encrypt-fn] [:decrypt-fn decrypt-fn]
           [:blind-fn blind-fn] [:visible? visible?]]]
    (when-not (ifn? value)
      (throw (ex-info "Kotobase engine requires an explicit security control"
                      {:type :kotobase.engine/missing-control
                       :control control}))))
  (->Database storage ref-name encrypt-fn decrypt-fn blind-fn visible?
              max-retries))

(defn head [database]
  (-> (storage/-read-ref (:storage database) (:ref-name database))
      js/Promise.resolve
      (.then #(some-> % :cid))))

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

(defn- db-value [database]
  (let [backend (:storage database)
        fetch1 (async-get-fn backend)]
    (-> (head database)
        (.then
         (fn [current]
           (with-blocks
             backend
             (fn [get-fn _put! _cas!]
               ;; Both caches nil: kotobase-peer documents that as byte-identical
               ;; to plain hydrate-chain. The only reason to reach for the -cached
               ;; arity is that it is the one that accepts async-get-fn.
               (peer/hydrate-chain-cached
                get-fn current (:blind-fn database) (:decrypt-fn database)
                nil nil fetch1))))))))

(defn datoms
  ([database] (datoms database nil))
  ([database options]
   (-> (db-value database)
       (.then #(peer/datoms % options (:visible? database))))))

(defn q [database pattern]
  (-> (db-value database)
      (.then #(peer/q % pattern (:visible? database)))))

(defn query
  ([database query] (query database query []))
  ([database query inputs]
   (-> (db-value database)
       (.then #(peer/query % query (:visible? database) inputs)))))

(defn pull [database entity pattern]
  (-> (db-value database)
      (.then #(peer/pull % entity pattern))))
