(ns kotobase.engine
  "Public JVM database engine over the provider-neutral storage contract.

  The historical kotobase-peer implementation is deliberately hidden behind
  this database-shaped API."
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

(defn head [^Database database]
  (some-> (storage/-read-ref (:storage database) (:ref-name database)) :cid))

(defn transact!
  [^Database database tx-data]
  (let [{:keys [put! get-fn cas!]} (storage/ports (:storage database))]
    (peer/commit-serialized!
     put! get-fn cas! (:ref-name database) (head database) tx-data
     (:encrypt-fn database) (:max-retries database))))

(defn- db-value [^Database database]
  (let [{:keys [get-fn]} (storage/ports (:storage database))]
    (peer/hydrate-chain get-fn (head database)
                        (:blind-fn database) (:decrypt-fn database))))

(defn datoms
  ([database] (datoms database nil))
  ([^Database database options]
   (peer/datoms (db-value database) options (:visible? database))))

(defn q [^Database database pattern]
  (peer/q (db-value database) pattern (:visible? database)))

(defn query
  ([database query] (query database query []))
  ([^Database database query inputs]
   (peer/query (db-value database) query (:visible? database) inputs)))

(defn pull [^Database database entity pattern]
  (peer/pull (db-value database) entity pattern))
