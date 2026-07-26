(ns run
  (:require [kotobase.core :as kotobase]
            [kotobase.storage.core :as storage]
            [kotobase.storage.memory :as memory]))

(let [backend (memory/memory-store)
      database
      (kotobase/open
       {:storage backend
        :encrypt-fn #(js/Promise.resolve %)
        :decrypt-fn #(js/Promise.resolve %)
        :blind-fn #(js/Promise.resolve (pr-str %))
        :visible? (constantly true)})]
  (-> (kotobase/transact! database [["alice" "role" "admin"]])
      (.then
       (fn [committed]
         (when-not (string? committed)
           (throw (js/Error. "transaction did not return a CID")))
         (-> (storage/-read-ref backend "main")
             js/Promise.resolve
             (.then
              (fn [ref]
                (when-not (= committed (:cid ref))
                  (throw (js/Error.
                          "transaction ref was not published")))
                (println "Async engine transaction: ok"))))))
      (.catch
       (fn [error]
         (js/console.error error)
         (js/process.exit 1)))))
