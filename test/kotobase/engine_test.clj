(ns kotobase.engine-test
  (:require [clojure.test :refer [deftest is]]
            [kotobase.datomic :as d]
            [kotobase.engine :as engine]
            [kotobase.storage.memory :as memory]))

(deftest open-fails-closed-without-security-controls
  (is (thrown? clojure.lang.ExceptionInfo
               (engine/open {:storage (memory/memory-store)}))))

(deftest public-engine-round-trips-a-transaction
  (let [database (engine/open
                  {:storage (memory/memory-store)
                   :encrypt-fn identity
                   :decrypt-fn identity
                   :blind-fn pr-str
                   :visible? (constantly true)})
        committed (engine/transact!
                   database [["alice" "role" "admin"]])]
    (is (string? committed))
    (is (= #{{:s "alice" :p "role" :o "admin"}}
           (engine/q database ["alice" "role" nil])))))

(deftest datomic-query-syntax-and-argument-order
  (let [database (engine/open
                  {:storage (memory/memory-store)
                   :encrypt-fn identity
                   :decrypt-fn identity
                   :blind-fn pr-str
                   :visible? (constantly true)})]
    (is (string?
         (:db-after
           (d/transact
           database
           {:tx-data [{:db/id "alice"
                       :person/name "Alice"
                       :person/role "admin"}
                      {:db/id "bob" :person/name "Bob"}]}))))
    (is (= #{["alice" "Alice"]}
           (d/q '[:find ?e ?name
                  :where
                  [?e :person/role "admin"]
                  [?e :person/name ?name]]
                (d/db database))))
    (is (= "Alice"
           (d/q '[:find ?name .
                  :where ["alice" :person/name ?name]]
                database)))
    (is (= ["Alice" "Bob"]
           (sort
            (d/q '[:find [?name ...]
                   :where [_ :person/name ?name]]
                 database))))
    (is (= [{:person "alice" :name "Alice"}]
           (d/q '[:find ?person ?name
                  :keys person name
                  :where
                  [?person :person/role "admin"]
                  [?person :person/name ?name]]
                database)))
    (is (= #{["alice"]}
           (d/q '[:find ?e
                  :in $ ?role
                  :where [?e :person/role ?role]]
                database "admin")))
    (is (= #{:e :a :v_edn :added}
           (set (keys (first (d/datoms database
                                       {:index :eavt
                                        :components ["alice"]
                                        :limit 1}))))))
    (is (= #{":person/name"}
           (set (keys (d/pull database [:person/name] "alice")))))))
