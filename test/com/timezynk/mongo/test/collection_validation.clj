(ns com.timezynk.mongo.test.collection-validation
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoWriteException]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest with-validation
  (m/create-collection! :users :validation {:$or [{:name {:$ne nil} :address {:$exists 0}}
                                                  {:name {:$exists 0} :address {:$ne nil}}]})
  (testing "Inserts that should pass validation"
    (try
      (m/insert! :users {:name "N"})
      (m/insert! :users {:address "A"})
      (catch Exception _e
        (is false))))
  (testing "Inserts that don't pass validation"
    (is (thrown-with-msg? MongoWriteException
                          #"Document failed validation"
                          (m/insert! :users {:name "N"
                                             :address "A"})))))
