(ns com.timezynk.mongo.test.insert
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [java.util.concurrent CountDownLatch TimeUnit]
           [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest insert
  (let [res (m/insert! :users {:name "Name"})]
    (is (= ObjectId
           (-> res :_id type)))
    (is (= "Name" (:name res))))
  (let [res (m/insert! :users [{:name "1"}
                               {:name "2"}])]
    (is (= 2 (count res)))))

(deftest insert-recursive
  (testing "Correct conversion of data tructure"
    (m/insert! :companies {:name "Name"
                           :address {:street "Main street"
                                     :number 12345
                                     :id (ObjectId.)}})
    (let [{:keys [_id name address]} (m/fetch-one :companies {})
          {:keys [street number id]} address]
      (is (= ObjectId (type _id)))
      (is (= "Name" name))
      (is (= "Main street" street))
      (is (= 12345 number))
      (is (= ObjectId (type id))))))

(defn write-thread-1 [latch-1 latch-2 latch-3]
  (m/transaction
   (m/insert! :companies {:duplicity 2})
   (.countDown latch-1)
   (is true? (.await latch-2 5 (TimeUnit/SECONDS)))
   (m/insert! :companies {:duplicity 3}))
  (.countDown latch-3))

(deftest transaction-insert
  (let [latch-1 (CountDownLatch. 1)
        latch-2 (CountDownLatch. 1)
        latch-3 (CountDownLatch. 1)]
    (async/thread
      (write-thread-1 latch-1 latch-2 latch-3))
    (testing "Make one insert in the transaction"
      (is true? (.await latch-1 5 (TimeUnit/SECONDS)))
      (is (= 0 (count (m/fetch :companies {})))))
    (.countDown latch-2)
    (testing "Make next insert and end transaction"
      (is true? (.await latch-3 5 (TimeUnit/SECONDS)))
      (is (= 2 (count (m/fetch :companies {})))))))

(deftest abort-transaction
  (testing "Aborted transaction makes no inserts"
    (try
      (m/drop-collection! :companies)
      (catch Exception _e))
    (m/create-collection! :companies :schema {:name (s/string)})
    (try
      (m/transaction
       (m/insert! :companies [{:name "1"}])
       (m/insert! :companies [{:address "A"}]))
      (catch Exception _e))
    (is (= [] (m/fetch :companies)))))
