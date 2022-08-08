(ns com.timezynk.mongo.test.watch
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.watch :as w]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(defn on-insert [time param]
  (log/spy "1111111111111")
  (log/spy time)
  (log/spy param))

(deftest list-coll
  (is true)
  (mongo/create-collection! :coll)
  (let [chan (w/insert :coll on-insert)]
    (mongo/insert! :coll {:name "Name1"})
    (mongo/insert! :coll {:name "Name2"})
    (Thread/sleep 1000)
    (log/spy "2222222222222")
    #_(log/spy (async/<!! chan))))

(deftest test-2
  (is true)
  (let [n (let [i (iterate inc 0)]
            (fn [] (next i)))]
    (log/spy n)))