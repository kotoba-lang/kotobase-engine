(ns kotobase.datomic-client-test
  "Maturity suite for the official Client API surface."
  (:require [clojure.test :refer [deftest is]]
            [kotobase.datomic.client :as d]))

(defn- fresh-conn []
  (let [client (d/client {:server-type :kotobase-local :system "test"})]
    (d/create-database client {:db-name "mbrainz-sample"})
    (d/connect client {:db-name "mbrainz-sample"})))

(deftest client-lifecycle-matches-datomic-client-api
  (let [client (d/client {:server-type :kotobase-local :system "life"})
        _a (d/create-database client {:db-name "a"})
        _b (d/create-database client {:db-name "b"})
        names-before (d/list-databases client {})
        conn (d/connect client {:db-name "a"})
        _del (d/delete-database client {:db-name "b"})
        names-after (d/list-databases client {})]
    (is (true? _a))
    (is (true? _b))
    (is (= ["a" "b"] names-before))
    (is (= "a" (:db-name conn)))
    (is (some? (d/db conn)))
    (is (true? _del))
    (is (= ["a"] names-after))
    (is (thrown? clojure.lang.ExceptionInfo
                 (d/administer-system client {:action :upgrade-schema})))))

(deftest transact-q-pull-datoms-client-shapes
  (let [conn (fresh-conn)
        tx (d/transact
            conn
            {:tx-data [{:db/id "alice" :person/name "Alice" :person/role "admin"}
                       {:db/id "bob" :person/name "Bob" :person/role "member"}]})
        db (d/db conn)
        q1 (d/q '[:find ?e ?name
                  :where
                  [?e :person/role "admin"]
                  [?e :person/name ?name]]
                db)
        q2 (d/q {:query '[:find ?e ?name
                          :where
                          [?e :person/role "admin"]
                          [?e :person/name ?name]]
                 :args [db]})
        q3 (d/q {:query '[:find ?e ?name
                          :where [_ :person/name ?name]
                          [?e :person/name ?name]]
                 :args [db]
                 :limit 1})
        entity (d/pull db [:person/name :person/role] "alice")
        row (first (d/datoms db {:index :eavt :components ["alice"]}))
        speculative (d/with (d/with-db conn)
                            {:tx-data [{:db/id "carol" :person/name "Carol"}]})
        durable (d/q '[:find ?name :where [_ :person/name ?name]]
                     (d/db conn))]
    (is (contains? tx :db-before))
    (is (contains? tx :db-after))
    (is (map? (:tempids tx)))
    (is (= #{["alice" "Alice"]} q1))
    (is (= #{["alice" "Alice"]} q2))
    (is (= 1 (count q3)))
    (is (seq (d/qseq '[:find ?e :where [?e :person/name _]] db)))
    (is (= "Alice" (or (:person/name entity) (get entity :person/name))))
    (is (keyword? (:a row)))
    (is (contains? row :v))
    (is (contains? row :tx))
    (is (contains? row :added))
    (is (seq (d/seek-datoms db {:index :aevt :components [:person/name]})))
    (is (seq (d/index-range db {:attrid :person/name :start "A" :end "C"})))
    (is (seq (d/index-pull db {:index :avet :selector [:person/name] :start [:person/name]})))
    (is (number? (:datoms (d/db-stats db))))
    (is (some? (d/history db)))
    (is (some? (d/as-of db (:t db))))
    (is (contains? speculative :db-after))
    (is (= #{["Alice"] ["Bob"]} durable))))

(deftest sync-and-tx-range-exist
  (let [conn (fresh-conn)
        _ (d/transact conn {:tx-data [{:db/id "x" :n 1}]})
        synced (d/sync conn 0)
        txs (d/tx-range conn {:start 0})]
    (is (some? synced))
    (is (vector? txs))))
