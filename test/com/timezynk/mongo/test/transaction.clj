(ns com.timezynk.mongo.test.transaction
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as mongo2]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(defn- fetch-and-update-thread [latch-1 latch-2]
  (let [res (mongo2/fetch-one :coll {:flag "start"})]
    (.countDown latch-1)
    (is true? (.await latch-2 5 TimeUnit/SECONDS))
    (mongo2/set! :coll
                 {:_id (:_id res)}
                 {:flag "fail"})
    res))

(defn- fetch-and-update-main []
  (let [res (mongo2/fetch-one :coll {:flag "start"})]
    (mongo2/set! :coll
                 {:_id (:_id res)}
                 {:flag "succeed"})))

(deftest transaction-update-race-fail
  (testing "Incorrect filtering on update"
    (mongo2/insert! :coll {:flag "start"})
    (let [latch-1 (CountDownLatch. 1)
          latch-2 (CountDownLatch. 1)
          latch-3 (CountDownLatch. 1)]
      (async/go
        (testing "Thread reads first but updates second, only one update should be allowed"
          (is (fetch-and-update-thread latch-1 latch-2)))
        (.countDown latch-3))
      (is true? (.await latch-1 5 TimeUnit/SECONDS))
      (testing "Updates first, should prevent thread from reading and updating anything"
        (fetch-and-update-main))
      (.countDown latch-2)
      (is true? (.await latch-3 5 TimeUnit/SECONDS))
      (is (= "fail"
             (:flag (mongo2/fetch-one :coll)))))))

(deftest transaction-update-race-succeed
  (testing "Correct filtering on update"
    (mongo2/insert! :coll {:flag "start"})
    (let [latch-1 (CountDownLatch. 1)
          latch-2 (CountDownLatch. 1)
          latch-3 (CountDownLatch. 1)]
      (async/go
        (testing "Thread runs in transaction, prevents read and update because it finishes second"
          (is (nil? (mongo2/transaction
                      (fetch-and-update-thread latch-1 latch-2)))))
        (.countDown latch-3))
      (mongo2/transaction
        (is true? (.await latch-1 5 TimeUnit/SECONDS))
        (testing "This transaction finishes first, prevents thread from doing anything"
          (fetch-and-update-main)))
      (.countDown latch-2)
      (is true? (.await latch-3 5 TimeUnit/SECONDS))
      (is (= "succeed"
             (:flag (mongo2/fetch-one :coll)))))))
