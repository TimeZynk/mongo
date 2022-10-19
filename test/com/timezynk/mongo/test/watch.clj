(ns com.timezynk.mongo.test.watch
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.watch :as w]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [java.util Date]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest test-insert
  (testing "Listen for inserts, ignore the update"
    (let [times (atom [])
          res (atom [])
          on-insert (fn [time doc]
                      (reset! times (concat @times [time]))
                      (reset! res (concat @res [doc])))
          _ (m/create-collection! :coll)
          _ (w/on-insert :coll on-insert)
          id-1 (:_id (m/insert! :coll {:name "Name1"}))
          id-2 (:_id (m/insert! :coll {:name "Name2"}))]
      (m/update-one! :coll {:name "Name1"} {:$set {:name "Name3"}})
      (Thread/sleep 1000)
      (is (= [Date Date] (map type @times)))
      (is (= [{:_id id-1 :name "Name1"}
              {:_id id-2 :name "Name2"}]
             @res)))))

(deftest test-update
  (testing "Listen for update, ignore the insert"
    (let [res (atom [])
          on-update (fn [_ doc]
                      (reset! res (concat @res [doc])))
          _ (m/create-collection! :coll)
          _ (w/on-update :coll on-update)
          id (:_id (m/insert! :coll {:name "Name1"}))]
      (m/update-one! :coll {:name "Name1"} {:$set {:name "Name3"}})
      (Thread/sleep 1000)
      (is (= [{:_id id :name "Name3"}]
             @res)))))

(deftest test-delete
  (testing "Listen for delete, ignore the insert"
    (let [res (atom [])
          on-delete (fn [_ doc]
                      (reset! res (concat @res [doc])))
          _ (m/create-collection! :coll)
          _ (w/on-delete :coll on-delete)
          id (:_id (m/insert! :coll {:name "Name1"}))]
      (m/delete-one! :coll {:name "Name1"})
      (Thread/sleep 1000)
      (is (= [{:_id id}]
             @res)))))
