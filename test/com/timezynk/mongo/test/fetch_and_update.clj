(ns com.timezynk.mongo.test.fetch-and-update
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoCommandException]
           [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest bad-update
  (testing "Update with nil"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Update can not be null"
                          (m/fetch-and-update-one! :coll
                                                   {}
                                                   nil))))
  (testing "Update with empty list"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Invalid pipeline for an update. The pipeline can not be empty."
                          (m/fetch-and-update-one! :coll
                                                   {}
                                                   [])))
    (is (thrown-with-msg? IllegalArgumentException
                          #"Invalid pipeline for an update. The pipeline can not be empty."
                          (m/fetch-and-update-one! :coll
                                                   {}
                                                   '()))))
  (testing "Update requires valid modifier"
    (is (thrown-with-msg? IllegalArgumentException
                          #"All update operators must start with '\$', but 'email' does not"
                          (m/fetch-and-update-one! :coll
                                                   {}
                                                   {:email "test@test.com"}))))
  (testing "Pipeline with wrong stage"
    (is (thrown-with-msg? MongoCommandException
                          #"Unrecognized pipeline stage name"
                          (m/fetch-and-update-one! :coll
                                                   {}
                                                   [{:$push {:b 1}}])))))

(deftest update-and-fetch-old
  (m/insert! :companies {:name "1"})
  (let [res (m/fetch-and-update-one! :companies
                                     {}
                                     {:$set {:name "2"}})]
    (is (= "1" (:name res)))
    (is (= "2" (:name (m/fetch-one :companies))))
    (is (= 1 (m/fetch-count :companies)))))

(deftest update-and-fetch-new
  (m/insert! :companies {:name "1"})
  (let [res (m/fetch-and-update-one! :companies
                                     {}
                                     {:$set {:name "2"}}
                                     :return-new? true)]
    (is (= "2" (:name res)))
    (is (= 1 (m/fetch-count :companies)))))

(deftest upsert-and-fetch-old
  (is (nil? (m/fetch-and-update-one! :companies
                                     {}
                                     {:$set {:name "2"}}
                                     :upsert? true)))
  (is (= "2" (:name (m/fetch-one :companies))))
  (is (= 1 (m/fetch-count :companies))))

(deftest upsert-and-fetch-new
  (let [res (m/fetch-and-update-one! :companies
                                     {}
                                     {:$set {:name "2"}}
                                     :upsert? true
                                     :return-new? true)]
    (is (= ObjectId (-> res :_id type)))
    (is (= "2" (:name res)))
    (is (= 1 (m/fetch-count :companies)))))
