(ns com.timezynk.mongo.test.insert
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [java.util.concurrent CountDownLatch TimeUnit]
           [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest insert
  (let [res (mongo/insert! :users {:name "Name"})]
    (is (= ObjectId
           (-> res :_id type)))
    (is (= "Name" (:name res))))
  (let [res (mongo/insert! :users [{:name "1"}
                                   {:name "2"}])]
    (is (= 2 (count res)))))

(deftest insert-recursive
  (testing "Correct conversion of data tructure"
    (mongo/insert! :companies {:name "Name"
                               :address {:street "Main street"
                                         :number 12345
                                         :id (ObjectId.)}})
    (let [{:keys [_id name address]} (mongo/fetch-one :companies {})
          {:keys [street number id]} address]
      (is (= ObjectId (type _id)))
      (is (= "Name" name))
      (is (= "Main street" street))
      (is (= 12345 number))
      (is (= ObjectId (type id))))))

(defn write-thread-1 [latch-1 latch-2 latch-3]
  (mongo/transaction
   (mongo/insert! :companies {:duplicity 2})
   (.countDown latch-1)
   (is true? (.await latch-2 5 (TimeUnit/SECONDS)))
   (mongo/insert! :companies {:duplicity 3}))
  (.countDown latch-3))

(deftest transaction-insert
  (let [latch-1 (CountDownLatch. 1)
        latch-2 (CountDownLatch. 1)
        latch-3 (CountDownLatch. 1)]
    (async/thread
      (write-thread-1 latch-1 latch-2 latch-3))
    (is true? (.await latch-1 5 (TimeUnit/SECONDS)))
    (is (= 0 (count (mongo/fetch :companies {}))))
    (.countDown latch-2)
    (is true? (.await latch-3 5 (TimeUnit/SECONDS)))
    (is (= 2 (count (mongo/fetch :companies {}))))))

(defn write-thread-2 [latch-1 latch-2 latch-3]
  (.countDown latch-1)
  (is true? (.await latch-2 5 (TimeUnit/SECONDS)))
  (mongo/insert! :companies {:duplicity 3})
  (.countDown latch-3))

(deftest transaction-fetch-1
  (testing "Fetching from inside a transaction with consecutive reads"
    (let [latch-1 (CountDownLatch. 1)
          latch-2 (CountDownLatch. 1)
          latch-3 (CountDownLatch. 1)]
      (async/thread
        (write-thread-2 latch-1 latch-2 latch-3))
      (is true? (.await latch-1 5 (TimeUnit/SECONDS)))
      (mongo/transaction
       (is (= 0 (count (mongo/fetch :companies {}))))
       (.countDown latch-2)
       (is true? (.await latch-3 5 (TimeUnit/SECONDS)))
       (is (= 0 (count (mongo/fetch :companies {}))))))))

(deftest transaction-fetch-2
  (testing "Fetching from inside a transaction but with single read"
    (let [latch-1 (CountDownLatch. 1)
          latch-2 (CountDownLatch. 1)
          latch-3 (CountDownLatch. 1)]
      (async/thread
        (write-thread-2 latch-1 latch-2 latch-3))
      (is true? (.await latch-1 5 (TimeUnit/SECONDS)))
      (mongo/transaction
       (.countDown latch-2)
       (is true? (.await latch-3 5 (TimeUnit/SECONDS)))
       (is (= 1 (count (mongo/fetch :companies {}))))))))
