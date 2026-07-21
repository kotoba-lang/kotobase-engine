(ns kotobase-peer.istore-adapter-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.istore.adapter :as adapter]))

;; ============================================================================
;; M6 IStore Adapter Tests
;; ============================================================================

(deftest m6-consumer-inventory-lists-dependencies
  (testing "istore-consumer-inventory should enumerate all consumers"
    (let [inventory (adapter/istore-consumer-inventory)]
      (is (pos? (count inventory)))
      (is (every? :consumer-name inventory))
      (is (every? :migration-status inventory)))))

(deftest m6-istore-to-datom-maps-operations
  (testing "migrate-istore-to-datom should translate operations correctly"
    (let [put-op {:op :put :doc-id "doc1" :value "content"}
          get-op {:op :get :doc-id "doc1"}
          list-op {:op :list}
          append-op {:op :append :stream-id "stream1" :value "event"}
          read-op {:op :read :stream-id "stream1"}]
      (is (= :datom/assert (:operation (adapter/migrate-istore-to-datom put-op))))
      (is (= :datom/query (:operation (adapter/migrate-istore-to-datom get-op))))
      (is (= :datom/range-scan (:operation (adapter/migrate-istore-to-datom list-op))))
      (is (= :datom/assert (:operation (adapter/migrate-istore-to-datom append-op))))
      (is (= :datom/range-scan (:operation (adapter/migrate-istore-to-datom read-op)))))))

(deftest m6-read-only-adapter-blocks-writes
  (testing "istore-read-only-adapter should block write operations"
    (let [put-req {:operation :put :doc-id "d" :value "v"}
          get-req {:operation :get :doc-id "d"}
          append-req {:operation :append :stream-id "s" :value "v"}

          put-result (adapter/istore-read-only-adapter put-req)
          get-result (adapter/istore-read-only-adapter get-req)
          append-result (adapter/istore-read-only-adapter append-req)]
      (is (= :read-only (:error put-result)))
      (is (nil? (:error get-result)))
      (is (= :datom-pull (:type get-result)))
      (is (= :read-only (:error append-result))))))

(deftest m6-cutover-plan-defines-complete-sequence
  (testing "cutover-plan should define all 6 cutover steps"
    (let [old-manifest (lsm/build-manifest {:db-id "test" :epoch 100})
          new-manifest (lsm/build-manifest {:db-id "test" :epoch 101})
          plan (adapter/cutover-plan old-manifest new-manifest)]
      (is (= 6 (count plan)))
      (is (every? :step plan))
      (is (every? :name plan))
      (is (= :checkpoint-istore (:name (first plan))))
      (is (= :delete-istore-code (:name (last plan)))))))

(deftest m6-execute-cutover-step-processes-steps
  (testing "execute-cutover-step should handle each step type"
    (let [checkpoint-step {:step 1 :name :checkpoint-istore :checkpoint-cid "cid1"}
          freeze-step {:step 2 :name :freeze-writes}
          flip-step {:step 4 :name :flip-to-merkle-lsm :new-manifest-cid "cid2"}

          checkpoint-result (adapter/execute-cutover-step checkpoint-step {})
          freeze-result (adapter/execute-cutover-step freeze-step {})
          flip-result (adapter/execute-cutover-step flip-step {})]
      (is (= :completed (:status checkpoint-result)))
      (is (= :completed (:status freeze-result)))
      (is (= :completed (:status flip-result))))))

(deftest m6-read-only-adapter-allows-reads
  (testing "istore-read-only-adapter should allow read operations"
    (let [get-req {:operation :get :doc-id "doc1"}
          read-req {:operation :read :stream-id "stream1"}
          list-req {:operation :list}

          get-result (adapter/istore-read-only-adapter get-req)
          read-result (adapter/istore-read-only-adapter read-req)
          list-result (adapter/istore-read-only-adapter list-req)]
      (is (nil? (:error get-result)))
      (is (= :datom-pull (:type get-result)))
      (is (nil? (:error read-result)))
      (is (= :datom-range (:type read-result)))
      (is (nil? (:error list-result))))))

(deftest m6-migrate-istore-to-datom-nil-op
  (testing "migrate-istore-to-datom should return nil for unknown op"
    (let [unknown-op {:op :unknown}
          result (adapter/migrate-istore-to-datom unknown-op)]
      (is (nil? result)))))

(deftest m6-istore-read-only-adapter-invalid-operation
  (testing "istore-read-only-adapter should handle invalid operations"
    (let [invalid-req {:operation :invalid}
          result (adapter/istore-read-only-adapter invalid-req)]
      (is (= :invalid-operation (:error result)))
      (is (string? (:message result))))))
