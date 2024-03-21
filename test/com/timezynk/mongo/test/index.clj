(ns com.timezynk.mongo.test.index
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest create
  (m/insert! :coll
             [{:name "1" :username "1"}
              {:name "2" :username "2"}])
  (m/create-index! :coll {:name 1 :username 1} :unique true)
  (is (= #{{:_id 1}
           {:name 1 :username 1}}
         (->> (m/list-indexes :coll)
              (map :key)
              (into #{})))))

(deftest shorthand
  (m/insert! :coll
             [{:name "1"}
              {:name "2" :username "2"}])
  (m/create-index! :coll {:name 1 :username 1} :unique true)
  (is (= #{{:_id 1}
           {:name 1 :username 1}}
         (->> (m/list-indexes :coll)
              (map :key)
              (into #{})))))

(deftest partial-filter
  (m/insert! :coll
             [{:name "1" :flag-index true}
              {:name "2" :flag-index false}])
  (m/create-index! :coll {:name 1} :filter {:flag-index true})
  (is (= #{{:key {:_id 1}}
           {:key {:name 1}
            :partialFilterExpression {:flag-index true}}}
         (->> (m/list-indexes :coll)
              (map #(dissoc % :v :name))
              (into #{})))))
