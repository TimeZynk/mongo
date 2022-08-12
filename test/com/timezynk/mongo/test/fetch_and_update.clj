(ns com.timezynk.mongo.test.fetch-and-update
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
  ;;  [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest update-and-fetch-old
  (mongo/insert! :companies {:name "1"})
  (is (= "1" (:name (mongo/fetch-and-update-one! :companies {} {:$set {:name "2"}}))))
  (is (= "2" (:name (mongo/fetch-one :companies {})))))

(deftest update-and-fetch-new
  (mongo/insert! :companies {:name "1"})
  (is (= "2" (:name (mongo/fetch-and-update-one! :companies {} {:$set {:name "2"}} :return-new? true)))))

(deftest upsert-and-fetch-old
  (is (nil? (:name (mongo/fetch-and-update-one! :companies {} {:$set {:name "2"}} :upsert? true))))
  (is (= "2" (:name (mongo/fetch-one :companies {})))))

(deftest upsert-and-fetch-new
  (is (= "2" (:name (mongo/fetch-and-update-one! :companies {} {:$set {:name "2"}} :upsert? true :return-new? true)))))
