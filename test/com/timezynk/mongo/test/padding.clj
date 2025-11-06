(ns com.timezynk.mongo.test.padding
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.padding :as mp]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest insert-padding
  (let [ts (System/currentTimeMillis)]
    (mp/with-padding {:insert (fn [doc] (assoc doc :created ts))}
      (is (= ts (:created (m/insert! :coll {:a 1})))))))

(deftest update-padding
  (let [ts (System/currentTimeMillis)]
    (mp/with-padding {:update (fn [doc] (assoc-in doc [:$set :updated] ts))}
      (m/update! :coll {:a 1} {:$set {:a 2}} :upsert? true)
      (is (= ts (:updated (m/fetch-one :coll {:a 2})))))))

(deftest fetch-and-update-padding
  (let [ts (System/currentTimeMillis)]
    (mp/with-padding {:update (fn [doc] (assoc-in doc [:$set :updated] ts))}
      (is (= ts (:updated (m/fetch-and-set-one! :coll {:a 2} {:a 3} :upsert? true :return-new? true)))))))

(deftest replace-padding
  (let [ts (System/currentTimeMillis)]
    (mp/with-padding {:replace (fn [doc] (assoc doc :replaced ts))}
      (m/replace-one! :coll {:a 1} {:a 2} :upsert? true)
      (is (= ts (:replaced (m/fetch-one :coll {:a 2})))))))
