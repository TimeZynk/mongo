(ns com.timezynk.mongo.test.conversion
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest has-id
  (testing "InsertOne returns with id"
    (let [res (m/insert! :coll {:a 1})]
      (is (= ObjectId (-> res :_id type)))
      (is (= (m/fetch-one :coll) res)))))

(deftest have-ids
  (testing "InsertMany returns with ids"
    (let [res (m/insert! :coll [{:a 1}])]
      (is (= ObjectId (-> res first :_id type)))
      (is (= (m/fetch :coll) res)))))

(deftest test-keyword
  (testing "Keyword is converted to string"
    (m/insert! :coll
               {:keyword :keyword})
    (is (= "keyword" (:keyword (m/fetch-one :coll))))))

(deftest test-namespace
  (testing "Keyword with a slash preserves the slash"
    (m/insert! :coll
               {:my/keyword :your/keyword})
    (is (= "your/keyword" (:my/keyword (m/fetch-one :coll))))))

(deftest test-set
  (testing "A clojure set is converted to vec"
    (m/insert! :coll
               {:set #{{:a #{:b}}}})
    (let [res (:set (m/fetch-one :coll))]
      (is (= [{:a ["b"]}] res)))))
