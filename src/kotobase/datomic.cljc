(ns kotobase.datomic
  "Datomic-shaped API and query grammar over kotobase-engine.

  This namespace deliberately uses Datomic's argument order:
  `(q query db & inputs)`, `(pull db selector eid)`, and
  `(datoms db opts)`. `kotobase.core` remains the smaller native facade."
  (:require [kotobase.engine :as engine]))

(def ^:private query-sections
  #{:find :with :in :where :keys :strs :syms :rules})

(defn- keyword-wire-value [value]
  (let [value-name (name value)]
    (if (.startsWith value-name "_")
      (str "_:"
           (when-let [value-namespace (namespace value)]
             (str value-namespace "/"))
           (subs value-name 1))
      (str value))))

(defn- wire-value [value]
  (cond
    (keyword? value) (keyword-wire-value value)
    (vector? value) (mapv wire-value value)
    (list? value) (apply list (map wire-value value))
    (set? value) (into #{} (map wire-value) value)
    (map? value) (into {}
                       (map (fn [[key item]]
                              [(wire-value key) (wire-value item)]))
                       value)
    :else value))

(defn- vector-query->map [query]
  (loop [tokens (seq query), section nil, result {}]
    (if-let [token (first tokens)]
      (if (contains? query-sections token)
        (recur (next tokens) token (assoc result token []))
        (if section
          (recur (next tokens) section (update result section conj token))
          (throw (ex-info "Datomic query must start with a query section"
                          {:type :kotobase.datomic/invalid-query
                           :query query}))))
      result)))

(defn query-map
  "Normalize Datomic's vector or map query representation to a query map."
  [query]
  (cond
    (map? query) query
    (vector? query) (vector-query->map query)
    :else
    (throw (ex-info "Datomic query must be a vector or map"
                    {:type :kotobase.datomic/invalid-query
                     :query query}))))

(defn- find-shape [find-spec]
  (cond
    (and (= 2 (count find-spec)) (= '. (second find-spec)))
    {:kind :scalar :engine-find [(first find-spec)]}

    (and (= 1 (count find-spec))
         (vector? (first find-spec))
         (= '... (second (first find-spec))))
    {:kind :collection :engine-find [(first (first find-spec))]}

    (and (= 1 (count find-spec)) (vector? (first find-spec)))
    {:kind :tuple :engine-find (vec (first find-spec))}

    :else
    {:kind :relation :engine-find (vec find-spec)}))

(defn- shape-results [kind results]
  (case kind
    :scalar (ffirst results)
    :collection (mapv first results)
    :tuple (first results)
    results))

(defn- named-results [query results]
  (let [[kind names]
        (cond
          (seq (:keys query)) [:keys (:keys query)]
          (seq (:strs query)) [:strs (:strs query)]
          (seq (:syms query)) [:syms (:syms query)]
          :else [nil nil])]
    (if-not kind
      results
      (mapv
       (fn [tuple]
         (into {}
               (map (fn [label value]
                      [(case kind
                         :keys (keyword (clojure.core/name label))
                         :strs (clojure.core/name label)
                         :syms (symbol (clojure.core/name label)))
                       value])
                    names tuple)))
       results))))

(defn compile-query
  "Compile Datomic query syntax to the engine query and result shape."
  [query]
  (let [query (query-map query)
        {:keys [kind engine-find]} (find-shape (:find query))
        query (cond-> query
                (:where query) (update :where wire-value)
                (:rules query) (update :rules wire-value))]
    {:query (assoc query :find engine-find)
     :shape kind
     :named? (boolean (some seq ((juxt :keys :strs :syms) query)))}))

(defn q
  "Datomic-compatible argument order: `(q query db & inputs)`.

  Supports relation, scalar (`.`), collection (`...`), tuple, and
  `:keys`/`:strs`/`:syms` result shapes."
  [query database & inputs]
  (let [{compiled :query shape :shape named? :named?}
        (compile-query query)
        finish (fn [results]
                 (let [results (if named?
                                 (named-results compiled results)
                                 results)]
                   (if named? results (shape-results shape results))))
        result (engine/query database compiled (mapv wire-value inputs))]
    #?(:clj (finish result)
       :cljs (.then result finish))))

(defn pull
  "Datomic-compatible argument order: `(pull db selector eid)`."
  [database selector eid]
  (engine/pull database (wire-value eid) (wire-value selector)))

(defn datoms
  "Datomic-compatible shape: `(datoms db {:index :eavt ...})`."
  ([database] (engine/datoms database))
  ([database options]
   (engine/datoms
    database
    (cond-> options
      (:components options) (update :components #(mapv wire-value %))))))

(defn db
  "Return the readable database handle. Kotobase handles resolve their head
  at operation time; unlike Datomic Peer this is not an immutable basis value."
  [connection]
  connection)

(defn- reverse-attribute [attribute]
  (when (and (keyword? attribute) (.startsWith (name attribute) "_"))
    (keyword (namespace attribute) (subs (name attribute) 1))))

(defn- entity-map-ops [entity]
  (let [entity-id (:db/id entity)]
    (when-not (some? entity-id)
      (throw
       (ex-info "Datomic entity maps require :db/id"
                {:type :kotobase.datomic/missing-db-id
                 :entity entity})))
    (mapcat
     (fn [[attribute value]]
       (when-not (= attribute :db/id)
         (let [values (if (and (coll? value) (not (map? value)))
                        value
                        [value])]
           (map
            (fn [item]
              (if-let [forward (reverse-attribute attribute)]
                [:db/add item forward entity-id]
                [:db/add entity-id attribute item]))
            values))))
     entity)))

(defn- normalize-tx-data [tx-data]
  (vec
   (mapcat
    (fn [item]
      (if (and (map? item) (contains? item :db/id))
        (entity-map-ops item)
        [item]))
    tx-data)))

(defn transact
  "Accept Datomic Client's `{:tx-data [...]}` shape or a raw tx-data seq.

  Returns a Promise on ClojureScript and a value on the JVM. The report names
  the immutable before/after head CIDs explicitly because Kotobase does not
  manufacture Datomic numeric t/tx entity ids."
  [connection tx]
  (let [requested-tx-data (if (map? tx) (:tx-data tx) tx)
        tx-data (normalize-tx-data requested-tx-data)
        before (engine/head connection)
        report (fn [before after]
                 {:db-before before
                  :db-after after
                  :tx-data requested-tx-data
                  :tempids {}})]
    #?(:clj
       (let [after (engine/transact! connection tx-data)]
         (report before after))
       :cljs
       (-> before
           (.then
            (fn [before-cid]
              (-> (engine/transact! connection tx-data)
                  (.then #(report before-cid %)))))))))
