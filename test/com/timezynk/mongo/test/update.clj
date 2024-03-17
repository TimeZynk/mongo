(ns com.timezynk.mongo.test.update
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoCommandException MongoWriteException]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest simple-update
  (testing "A simple update"
    (let [res (m/insert! :companies {:name "Company"})]
      (is (= {:matched-count 1
              :modified-count 1}
             (m/update! :companies
                        {:_id (:_id res)}
                        {:$set {:email "test@test.com"}})))
      (is (= "test@test.com"
             (:email (m/fetch-one :companies {})))))))

(deftest bad-update
  (testing "Update with nil"
    (is (thrown-with-msg? IllegalArgumentException
                          #"update can not be null"
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
  #_(testing "Update requires valid modifier"
      (is (thrown-with-msg? IllegalArgumentException
                            #"not a valid modifier: :email"
                            (m/update! :coll
                                       {}
                                       {:email "test@test.com"}))))
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
    (m/update! :companies {} {:$set {:name "Company"}})
    (is (= 0 (count (m/fetch :companies {}))))
    (m/update! :companies {} {:$set {:name "Company"}} :upsert? true)
    (is (= 1 (count (m/fetch :companies {}))))))

(deftest update-many
  (testing "Update one or many"
    (m/insert! :companies [{:name "Company 1"}
                           {:name "Company 2"}])
    (is (= {:matched-count 1
            :modified-count 1}
           (m/update-one! :companies {} {:$set {:name "Company 3"}})))
    (is (= {:matched-count 2
            :modified-count 2}
           (m/update! :companies {} {:$set {:name "Company 4"}})))))

(deftest transaction-update-order
  (testing "Check that transaction enforces update order"
    (m/insert! :companies {:name "1"})
    (let [write-thread (fn []
                         (Thread/sleep 1000)
                         (m/update! :companies {} {:$set {:name "2"}}))]
      (async/thread
        (write-thread))
      (m/transaction
       (m/update! :companies {} {:$set {:name "3"}})
       (Thread/sleep 2000)))
    (is (= "3" (:name (m/fetch-one :companies {}))))))

(deftest abort-transaction
  (testing "Aborted transaction makes no updates"
    (try
      (m/drop-collection! :companies)
      (catch Exception _e))
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
