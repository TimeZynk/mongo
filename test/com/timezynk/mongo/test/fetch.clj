(ns com.timezynk.mongo.test.fetch
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest query
  (m/insert! :companies
             [{:name "1"}
              {:name "2"}])
  (is (= 1 (count (m/fetch :companies {:name "1"})))))

(deftest test-count
  (m/insert! :companies
             [{:name "1"}
              {:name "2"}])
  (is (= 1 (m/fetch-count :companies {:name "1"}))))

(deftest fetch-options
  (m/insert! :companies
             [{:name "1" :username "user 1"}
              {:name "2" :username "user 2"}
              {:name "3" :username "user 3"}
              {:name "4" :username "user 4"}])
  (is (map? (m/fetch-one :companies)))
  (is (= 1 (count (m/fetch :companies
                           {}
                           :limit 1))))
  (is (= 2 (count (m/fetch :companies
                           {}
                           :skip 2))))
  (is (= [{:username "user 4"}]
         (m/fetch :companies
                  {:name "4"}
                  :only {:_id 0 :username 1})))
  (is (= [{:name "4"} {:name "3"} {:name "2"} {:name "1"}]
         (m/fetch :companies
                  {}
                  :only {:_id 0 :name 1}
                  :sort {:name -1})))
  (is (= {:name "1"}
         (m/fetch-one :companies
                      {}
                      :only {:_id 0 :name 1})))
  (is (= {:name "4"}
         (m/fetch-one :companies
                      {}
                      :only {:_id 0 :name 1}
                      :sort {:name -1}))))
