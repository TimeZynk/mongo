(ns com.timezynk.mongo.test.database
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
;;    [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest list-dbs
  (testing "List default databases"
    (is (= #{"admin" "config" "local"}
           (->> (mongo/list-databases)
                (map :name)
                (into #{}))))))

(deftest switch-db
  (testing "Switch between databases"
    (mongo/insert! :users {:name "1"})
    (mongo/with-database "test-2"
      (mongo/insert! :users {:name "2"})
      (is (= 1 (mongo/fetch-count :users)))
      (is (= "2" (:name (mongo/fetch-one :users)))))
    (is (= 1 (mongo/fetch-count :users)))
    (is (= "1" (:name (mongo/fetch-one :users)))))
  (testing "Cleanup"
    (mongo/with-database "test-2"
      (doseq [coll (mongo/list-collections)]
        (mongo/drop-collection! coll)))))