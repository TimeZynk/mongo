(ns com.timezynk.mongo.test.index
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest create
  (mongo/insert! :companies
                 [{:name "1" :username "1"}
                  {:name "2" :username "2"}])
  (is true)
  (mongo/create-index! :companies {:name 1 :username 1} :unique true)
  (log/spy (mongo/list-indexes :companies)))
