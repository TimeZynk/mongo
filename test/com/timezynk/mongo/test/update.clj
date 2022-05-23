(ns com.timezynk.mongo.test.update
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing use-fixtures]]
  ;;  [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [java.util.concurrent CyclicBarrier]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest simple-update
  (let [res (mongo/insert! :companies {:name "Company"})]
    (is (= {:matched-count 1
            :modified-count 1}
           (mongo/update! :companies
                          {:_id (:_id res)}
                          {:$set {:email "test@test.com"}})))
    (is (= "test@test.com"
           (:email (mongo/fetch-one :companies {}))))))

(deftest bad-update
  (is (thrown-with-msg? Exception #"Invalid BSON field"
                        (mongo/update! :companies
                                       {}
                                       {:email "test@test.com"}))))

(deftest upsert
  (mongo/update! :companies {} {:$set {:name "Company"}})
  (is (= 0 (count (mongo/fetch :companies {}))))
  (mongo/update! :companies {} {:$set {:name "Company"}} :upsert? true)
  (is (= 1 (count (mongo/fetch :companies {})))))

(deftest update-many
  (mongo/insert! :companies [{:name "Company 1"}
                             {:name "Company 2"}])
  (is (= {:matched-count 1
          :modified-count 1}
         (mongo/update-one! :companies {} {:$set {:name "Company 3"}})))
  (is (= {:matched-count 2
          :modified-count 2}
         (mongo/update! :companies {} {:$set {:name "Company 4"}}))))

(defn write-thread-1 [latch]
    ;; (.await latch)
  (Thread/sleep 1000)
  (mongo/update! :companies {} {:$set {:name "2"}})
    ;; (.await latch)
  )

(deftest transaction-update
  (mongo/insert! :companies {:name "1"})
  (let [latch (CyclicBarrier. 2)]
    (async/thread
      (write-thread-1 latch))
    (mongo/transaction
     (mongo/update! :companies {} {:$set {:name "3"}})
     (Thread/sleep 2000)
      ;;  (.await latch)
      ;;  (.await latch)
     (is (= "3" (:name (mongo/fetch-one :companies {})))))))
