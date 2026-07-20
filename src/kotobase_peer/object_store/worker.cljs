(ns kotobase-peer.object-store.worker
  "S3-backed content-addressed block store. R2 uses its Worker binding; B2 uses
  the S3-compatible HTTP API with SigV4. Mutable head publication stays a
  separate operation because an immutable CAS block and compare-and-swap are
  different consistency contracts."
  (:require [clojure.string :as str]
            [goog.object :as gobj]
            [ipld.core :as ipld]
            [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.object-store.s3-sigv4 :as sigv4]))

(defn- env [e k] (gobj/get e k))
(defn- prefix [e]
  (str (str/replace (or (env e "MERKLE_S3_PREFIX") "kotobase/merkle-lsm") #"^/+|/+$" "") "/"))
(defn block-key [e cid] (str (prefix e) "blocks/" cid))
(defn head-key [e db-id] (str (prefix e) "heads/" db-id))

(defn- b2-config [e]
  (when (every? #(seq (env e %))
                ["MERKLE_S3_ENDPOINT" "MERKLE_S3_BUCKET"
                 "MERKLE_S3_ACCESS_KEY_ID" "MERKLE_S3_SECRET_ACCESS_KEY"])
    {:endpoint (env e "MERKLE_S3_ENDPOINT")
     :bucket (env e "MERKLE_S3_BUCKET")
     :region (or (env e "MERKLE_S3_REGION") "us-west-004")
     :access-key (env e "MERKLE_S3_ACCESS_KEY_ID")
     :secret-key (env e "MERKLE_S3_SECRET_ACCESS_KEY")}))

(defn configured? [e]
  (boolean (or (env e "MERKLE_BUCKET") (b2-config e))))

(defn put-block!
  "Idempotently put CID bytes into R2 or a configured S3-compatible bucket."
  [e cid bytes]
  (let [key (block-key e cid)]
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (.put bucket key bytes)
      (if-let [config (b2-config e)]
        (-> (sigv4/signed-headers
             (assoc config :method "PUT" :key key :body bytes))
            (.then (fn [{:keys [url headers]}]
                     (js/fetch url #js {:method "PUT" :headers headers :body bytes})))
            (.then (fn [response]
                     (if (.-ok response)
                       response
                       (js/Promise.reject
                        (js/Error. (str "S3 block PUT failed: " (.-status response))))))))
        (js/Promise.reject (js/Error. "No MERKLE_BUCKET or MERKLE_S3_* backend configured"))))))

(defn get-block!
  "Fetch immutable block bytes by CID from R2."
  [e cid]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.get bucket (block-key e cid))
        (.then (fn [obj]
                 (if obj
                   (-> (.arrayBuffer obj) (.then #(js/Uint8Array. %)))
                   (js/Promise.reject
                    (ex-info "Merkle block not found" {:cid cid}))))))
    (js/Promise.reject
     (js/Error. "Merkle reads require MERKLE_BUCKET R2 binding"))))

(defn get-node!
  "Fetch and DAG-CBOR decode an IPLD node."
  [e cid]
  (-> (get-block! e cid) (.then ipld/decode)))

(defn put-blocks!
  "Persist every :block/put effect concurrently. Runs and their manifest are
  immutable and independent; the caller publishes the mutable head only after
  this Promise resolves, preserving publication order without serial RTTs."
  [e effects]
  (->> effects
       (keep (fn [{:keys [effect/type cid bytes]}]
               (when (= :block/put type) (put-block! e cid bytes))))
       clj->js
       js/Promise.all))

(defn get-head
  "Read an R2 head with its ETag for a subsequent native conditional PUT."
  [e db-id]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.get bucket (head-key e db-id))
        (.then (fn [obj]
                 (if obj
                   (-> (.text obj)
                       (.then (fn [value] {:value value :etag (gobj/get obj "etag")})))
                   {:value nil :etag nil}))))
    (js/Promise.reject
     (js/Error. "Mutable Merkle head requires MERKLE_BUCKET R2 binding"))))

(defn cas-head!
  "R2-native compare-and-swap. Returns Promise<boolean>; nil ETag means
  create-if-absent. S3/B2 remains valid for immutable blocks but is not used
  for the mutable head because its conditional PUT contract is not portable."
  [e db-id next etag]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.put bucket (head-key e db-id) next
              #js {:onlyIf (if etag
                             #js {:etagMatches etag}
                             #js {:etagDoesNotMatch "*"})})
        (.then boolean))
    (js/Promise.reject
     (js/Error. "Mutable Merkle head requires MERKLE_BUCKET R2 binding"))))

(declare manifest-window! index-run-refs load-runs!)

(defn find-latest-entity!
  "Walk newest-first manifests and return the first EAVT datom set for entity.
  This is the correctness-oriented cutover reader; compaction/range indexes can
  later replace its bounded manifest walk without changing callers."
  ([e db-id entity] (find-latest-entity! e db-id entity 256))
  ([e db-id entity max-depth]
   (letfn [(scan [manifest-cid depth]
             (cond
               (nil? manifest-cid) (js/Promise.resolve nil)
               (>= depth max-depth)
               (js/Promise.reject
                (ex-info "Merkle manifest scan depth exceeded"
                         {:db-id db-id :entity entity :max-depth max-depth}))
               :else
               (-> (get-node! e manifest-cid)
                   (.then
                    (fn [manifest]
                      (let [refs (get-in manifest ["indexes" "eavt" "l0"])
                            previous (some-> (get manifest "previous") ipld/link-cid)
                            loads (mapv #(get-node! e (ipld/link-cid (get % "cid"))) refs)]
                        (-> (js/Promise.all (clj->js loads))
                            (.then
                             (fn [runs]
                               (let [rows (->> (array-seq runs)
                                               (mapcat #(get % "rows"))
                                               (filter #(= entity
                                                           (first (get % "components"))))
                                               vec)]
                                 (if (seq rows)
                                   rows
                                   (scan previous (inc depth)))))))))))))]
     (-> (get-head e db-id)
         (.then (fn [{:keys [value]}] (scan value 0)))))))

(defn find-entities!
  "Return {entity [EAVT rows]} for every entity whose id starts with PREFIX.
  Reads each manifest/run at most once. Intended for bounded list/event reads
  until range-directed L1 lookup lands."
  ([e db-id prefix] (find-entities! e db-id prefix 256))
  ([e db-id prefix max-depth]
   (-> (get-head e db-id)
       (.then
        (fn [{head-cid :value}]
          (-> (manifest-window! e head-cid max-depth)
              (.then
               (fn [{:keys [manifests tail]}]
                 (when tail
                   (throw (ex-info "Merkle entity scan depth exceeded"
                                   {:db-id db-id :prefix prefix
                                    :max-depth max-depth})))
                 (-> (load-runs! e (index-run-refs manifests :eavt))
                     (.then
                      (fn [runs]
                        (->> runs
                             (mapcat #(get-in % [:node "rows"]))
                             (filter (fn [row]
                                       (str/starts-with?
                                        (str (first (get row "components"))) prefix)))
                             (group-by #(first (get % "components")))))))))))))))

(defn- manifest-window!
  [e head-cid limit]
  (letfn [(step [cid remaining acc]
            (if (or (nil? cid) (zero? remaining))
              (js/Promise.resolve
               {:manifests acc :tail cid})
              (-> (get-node! e cid)
                  (.then (fn [node]
                           (step (some-> (get node "previous") ipld/link-cid)
                                 (dec remaining)
                                 (conj acc {:cid cid :node node})))))))]
    (step head-cid limit [])))

(defn- index-run-refs [manifests index]
  (mapcat (fn [{:keys [node]}]
            (mapcat val (get-in node ["indexes" (name index)])))
          manifests))

(defn- load-runs! [e refs]
  (-> (mapv (fn [ref]
              (-> (get-node! e (ipld/link-cid (get ref "cid")))
                  (.then (fn [node] {:node node}))))
            refs)
      clj->js
      js/Promise.all
      (.then #(vec (array-seq %)))))

(defn compact-head!
  "Compact the newest manifest window into one L1 run per present index and
  publish it with R2 CAS. The untouched tail remains linked as :previous.
  Returns Promise<boolean>; false means a concurrent writer won the head."
  ([e db-id] (compact-head! e db-id 64))
  ([e db-id window-size]
   (-> (get-head e db-id)
       (.then
        (fn [{head-cid :value :keys [etag]}]
          (if-not head-cid
            false
            (-> (manifest-window! e head-cid window-size)
                (.then
                 (fn [{:keys [manifests tail]}]
                   (let [present (filter #(seq (index-run-refs manifests %)) lsm/indexes)
                         loads (mapv #(load-runs! e (index-run-refs manifests %)) present)]
                     (-> (js/Promise.all (clj->js loads))
                         (.then
                          (fn [loaded]
                            (let [epoch (apply max (map #(get-in % [:node "epoch"]) manifests))
                                  compacted (into {}
                                                  (map (fn [index runs]
                                                         [index (lsm/compact-runs
                                                                 index db-id epoch runs)])
                                                       present (array-seq loaded)))
                                  manifest (lsm/build-manifest
                                            {:db-id db-id :epoch epoch :safe-epoch epoch
                                             :previous tail
                                             :indexes (into {}
                                                            (map (fn [[index run]]
                                                                   [index {:l1 [run]}]))
                                                            compacted)
                                             :statistics {"operation" "window-compaction"
                                                          "manifest-count" (count manifests)}})
                                  effects (concat (mapcat :effects (vals compacted))
                                                  (:effects manifest))]
                              (-> (put-blocks! e effects)
                                  (.then (fn [_]
                                           (cas-head! e db-id (:cid manifest) etag))))))))))))))))))
