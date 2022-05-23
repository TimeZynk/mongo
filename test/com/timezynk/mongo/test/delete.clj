(ns com.timezynk.mongo.test.delete
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
  ;;  [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest single-delete
  (testing "Create two docs, delete one"
    (mongo/insert! :companies [{:name "1"}
                               {:name "2"}])
    (is (= {:deleted-count 1}
           (mongo/delete-one! :companies {})))
    (is (= 1 (count (mongo/fetch :companies {}))))))

(deftest multiple-delete
  (testing "Create two docs, delete both"
    (mongo/insert! :companies [{:name "1"}
                               {:name "2"}])
    (is (= {:deleted-count 2}
           (mongo/delete! :companies {})))
    (is (= 0 (count (mongo/fetch :companies {}))))))
