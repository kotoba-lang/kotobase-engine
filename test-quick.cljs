(ns test-quick
  (:require [clojure.test :refer [deftest is testing]]
            [kotobase-peer.compaction :as compaction]))

;; Quick test of D1 fix: key-range-overlap?
(deftest d1-key-range-overlap-with-strings
  (testing "key-range-overlap? should work with string keys"
    (is (true? (compaction/key-range-overlap? ["a" "d"] ["c" "f"])))
    (is (true? (compaction/key-range-overlap? ["a" "d"] ["a" "d"])))
    (is (false? (compaction/key-range-overlap? ["a" "c"] ["d" "f"])))))

(when (undefined? process)
  (js/console.log "D1 tests passed"))

