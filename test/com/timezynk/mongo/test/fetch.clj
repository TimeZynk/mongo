(ns com.timezynk.mongo.test.fetch
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
  ;;  [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest query
  (mongo/insert! :companies
                 [{:name "1"}
                  {:name "2"}])
  (is (= 1 (count (mongo/fetch :companies {:name "1"})))))

(deftest test-count
  (mongo/insert! :companies
                 [{:name "1"}
                  {:name "2"}])
  (is (= 1 (mongo/fetch-count :companies {:name "1"}))))

(deftest fetch-options
  (mongo/insert! :companies
                 [{:name "1" :username "user 1"}
                  {:name "2" :username "user 2"}
                  {:name "3" :username "user 3"}
                  {:name "4" :username "user 4"}])
  (is (map? (mongo/fetch-one :companies)))
  (is (= 1 (count (mongo/fetch :companies {} :limit 1))))
  (is (= 2 (count (mongo/fetch :companies {} :skip 2))))
  (is (= [{:username "user 4"}]
         (mongo/fetch :companies {:name "4"} :only {:_id 0 :username 1})))
  (is (= [{:name "4"} {:name "3"} {:name "2"} {:name "1"}]
         (mongo/fetch :companies {} :only {:_id 0 :name 1} :sort {:name -1}))))
