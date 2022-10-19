(ns com.timezynk.mongo.test.database
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest list-dbs
  (testing "List default databases"
    (is (= #{"admin" "config" "local"}
           (->> (m/list-databases)
                (map :name)
                (into #{}))))))

(deftest switch-db
  (testing "Switch between databases"
    (m/insert! :users {:name "1"})
    (m/with-database "test-2"
      (m/insert! :users {:name "2"})
      (is (= 1 (m/fetch-count :users)))
      (is (= "2" (:name (m/fetch-one :users)))))
    (is (= 1 (m/fetch-count :users)))
    (is (= "1" (:name (m/fetch-one :users)))))
  (testing "Cleanup"
    (m/with-database "test-2"
      (doseq [coll (m/list-collection-names)]
        (m/drop-collection! coll)))))