(ns com.timezynk.mongo.test.update
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoCommandException MongoWriteException]
           [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest simple-update
  (testing "A simple update"
    (let [res (m/insert! :companies {:name "Company"})]
      (is (= {:matched-count 1
              :modified-count 1
              :_id nil
              :acknowledged true}
             (m/update! :companies
                        {:_id (:_id res)}
                        {:$set {:email "test@test.com"}})))
      (is (= "test@test.com"
             (:email (m/fetch-one :companies {})))))))

(deftest bad-update
  (testing "Update with nil"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Update can not be null"
                          (m/update! :coll
                                     {}
                                     nil))))
  (testing "Update with empty list"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Invalid pipeline for an update. The pipeline may not be empty."
                          (m/update! :coll
                                     {}
                                     [])))
    (is (thrown-with-msg? IllegalArgumentException
                          #"Invalid pipeline for an update. The pipeline may not be empty."
                          (m/update! :coll
                                     {}
                                     '()))))
  (testing "Update requires valid modifier"
    (is (thrown-with-msg? IllegalArgumentException
                          #"All update operators must start with '\$', but 'email' does not"
                          (m/update! :coll
                                     {}
                                     {:email "test@test.com"
                                      :name "Name"}))))
  (testing "Update with null value"
    (is (thrown-with-msg? MongoWriteException
                          #"Modifiers operate on fields but we found type null instead"
                          (m/update! :coll
                                     {}
                                     {:$set nil}))))
  (testing "Pipeline with wrong stage"
    (is (thrown-with-msg? MongoCommandException
                          #"Unrecognized pipeline stage name"
                          (m/update! :coll
                                     {}
                                     [{:$push {:b 1}}])))))

(deftest upsert
  (testing "Upsert creates a document"
    (let [res (m/update! :companies {} {:$set {:name "Company"}})]
      (is (nil? (-> res :_id)))
      (is (= 0 (count (m/fetch :companies {})))))
    (let [res (m/update! :companies {} {:$set {:name "Company"}} :upsert? true)]
      (is (= ObjectId (-> res :_id type)))
      (is (= 1 (count (m/fetch :companies {})))))))

(deftest update-many
  (testing "Update one or many"
    (m/insert! :companies [{:name "Company 1"}
                           {:name "Company 2"}])
    (is (= {:matched-count 1
            :modified-count 1
            :_id nil
            :acknowledged true}
           (m/update-one! :companies {} {:$set {:name "Company 3"}})))
    (is (= {:matched-count 2
            :modified-count 2
            :_id nil
            :acknowledged true}
           (m/update! :companies {} {:$set {:name "Company 4"}})))))

(deftest transaction-update-order
  (testing "Check that transaction enforces update order"
    (m/insert! :coll {:order 0})
    (testing "Without transaction, updates are in timed order"
      (async/go
        (Thread/sleep 500)
        (m/update! :coll {} {:$set {:order 2}}))
      (m/update! :coll {} {:$set {:order 1}})
      (Thread/sleep 1000)
      (m/update! :coll {} {:$set {:order 3}})
      (is (= 3 (:order (m/fetch-one :coll {})))))
    (testing "With transaction, collection lock enforces order"
      (async/go
        (Thread/sleep 500)
        (m/update! :coll {} {:$set {:order 2}}))
      (m/transaction
        (m/update! :coll {} {:$set {:order 1}})
        (Thread/sleep 1000)
        (m/update! :coll {} {:$set {:order 3}}))
      (is (= 2 (:order (m/fetch-one :coll {})))))))

(deftest abort-transaction
  (testing "Aborted transaction makes no updates"
    (m/create-collection! :companies :schema {:name (s/string)})
    (m/insert! :companies [{:name "1"}
                           {:name "2"}])
    (try
      (m/transaction
        (m/update! :companies {:name "1"} {:$set {:name "3"}})
        (m/update! :companies {:name "2"} {:$set {:name "4" :address "A"}}))
      (catch Exception _e))
    (is (= #{"1" "2"}
           (->> (m/fetch :companies)
                (map :name)
                (into #{}))))))

(deftest unacknowledged
  (m/with-write-concern :unacknowledged
    (is (= {:acknowledged false}
           (m/update! :coll {} {:$set {:a 1}})))))

(deftest hint
  (m/insert! :coll [{:a 1} {:a 2}])
  (m/update-one! :coll {} {:$set {:a 3}})
  (is (= [3 2] (->> (m/fetch :coll)
                    (map :a))))
  (m/create-index! :coll [:a])
  (m/update-one! :coll {} {:$set {:a 4}})
  (is (= [4 2] (->> (m/fetch :coll)
                    (map :a))))
  (m/update-one! :coll {} {:$set {:a 5}} :hint [:a])
  (is (= [4 5] (->> (m/fetch :coll)
                    (map :a)))))
