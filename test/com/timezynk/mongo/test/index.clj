(ns com.timezynk.mongo.test.index
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest create
  (m/insert! :companies
             [{:name "1" :username "1"}
              {:name "2" :username "2"}])
  (m/create-index! :companies {:name 1 :username 1} :unique true)
  (is (= #{{:_id 1}
           {:name 1 :username 1}}
         (->> (m/list-indexes :companies)
              (map :key)
              (into #{})))))
