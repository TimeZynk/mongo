(ns com.timezynk.mongo.test.delete
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest single-delete
  (testing "Create two docs, delete one"
    (m/insert! :companies [{:name "1"}
                           {:name "2"}])
    (is (= {:deleted-count 1
            :acknowledged true}
           (m/delete-one! :companies {})))
    (is (= 1 (count (m/fetch :companies))))))

(deftest multiple-delete
  (testing "Create two docs, delete both"
    (m/insert! :companies [{:name "1"}
                           {:name "2"}])
    (is (= {:deleted-count 2
            :acknowledged true}
           (m/delete! :companies {})))
    (is (= 0 (count (m/fetch :companies {}))))))
