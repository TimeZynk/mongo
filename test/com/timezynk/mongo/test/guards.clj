(ns com.timezynk.mongo.test.guards
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.assert :refer [catch-assert]]
   [com.timezynk.mongo.guards :as mg]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest valid-keys
  (testing "Valid guard keys"
    (is (= 0 (catch-assert
              (mg/with-guards {:insert ()
                               :update ()
                               :replace ()})))))
  (testing "Invalid guard key"
    (is (= 1 (catch-assert
              (mg/with-guards {:insert ()
                               :update ()
                               :replace ()
                               :a ()}))))))

(deftest insert-guard
  (testing "Default guard allows empty list"
    (let [res (m/insert! :coll [])]
      (is (= [] res))))
  (testing "No guard causes exception"
    (mg/with-guards {:insert (fn [_] true)}
      (is (thrown-with-msg? IllegalArgumentException
                            #"state should be: writes is not an empty list"
                            (m/insert! :coll []))))))
