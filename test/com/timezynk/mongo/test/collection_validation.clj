(ns com.timezynk.mongo.test.collection-validation
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest with-validation
  (mongo/create-collection! :users :validation {:$or [{:name {:$exists 1} :address {:$exists 0}}
                                                      {:name {:$exists 0} :address {:$exists 1}}]})
  (testing "Inserts that should pass validation"
    (try
      (mongo/insert! :users {:name "N"})
      (mongo/insert! :users {:address "A"})
      (catch Exception _e
        (is false))))
  (testing "Inserts that don't pass validation"
    (is (thrown-with-msg? Exception
                          #"Document failed validation"
                          (mongo/insert! :users {:name "N"
                                                 :address "A"})))))
