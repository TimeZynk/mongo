(ns com.timezynk.mongo.test.database
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.assert :refer [catch-assert]]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest switch-db
  (testing "Switch between databases"
    (try
      (m/insert! :users {:name "1"})
      (m/with-database :test-2
        (m/insert! :users {:name "2"})
        (is (= 1 (m/fetch-count :users)))
        (is (= "2" (:name (m/fetch-one :users)))))
      (is (= 1 (m/fetch-count :users)))
      (is (= "1" (:name (m/fetch-one :users))))
      (finally
        (m/with-database :test-2
          (m/drop-collection! :users))))))

(deftest write-concern
  (is (= 1 (catch-assert (m/with-write-concern :w4 ())))))
