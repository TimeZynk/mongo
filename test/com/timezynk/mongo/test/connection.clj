(ns com.timezynk.mongo.test.connection
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.util :as u]))

(deftest write-concern
  (is (thrown-with-msg? IllegalArgumentException
                        #"No matching clause: :w4"
                        (m/create-connection! "mongodb://localhost:27017/db" :write-concern :w4))))

(deftest disjoint
  (testing "Switch to another connection after exiting connection scope"
    (let [db-1 (m/create-connection! "mongodb://localhost:27017/db-1")
          db-2 (m/create-connection! "mongodb://localhost:27017/db-2")]
      (try
        (m/with-mongo db-1
          (is (= :db-1 (m/database-name))))
        (m/with-mongo db-2
          (is (= :db-2 (m/database-name))))
        (finally
          (m/close-connection! db-1)
          (m/close-connection! db-2))))
    (testing "Switch using convenience functions"
      (u/set-mongo-uri! "mongodb://localhost:27017/db-1")
      (u/wrap-mongo
        (is (= :db-1 (m/database-name))))
      (u/set-mongo-uri! "mongodb://localhost:27017/db-2")
      (u/wrap-mongo
        (is (= :db-2 (m/database-name)))))))

(deftest nested
  (testing "Switch to another connection inside the current connection scope"
    (let [db-1 (m/create-connection! "mongodb://localhost:27017/db-1")
          db-2 (m/create-connection! "mongodb://localhost:27017/db-2")]
      (try
        (m/with-mongo db-1
          (is (= :db-1 (m/database-name)))
          (m/with-mongo db-2
            (is (= :db-2 (m/database-name))))
          (is (= :db-1 (m/database-name))))
        (finally
          (m/close-connection! db-1)
          (m/close-connection! db-2))))
    (testing "Switch using convenience functions"
      (u/set-mongo-uri! "mongodb://localhost:27017/db-1")
      (u/wrap-mongo
        (is (= :db-1 (m/database-name)))
        (u/set-mongo-uri! "mongodb://localhost:27017/db-2")
        (u/wrap-mongo
          (is (= :db-2 (m/database-name))))
        (is (= :db-1 (m/database-name)))))))

(deftest unacknowledged
  (testing "Set operations to use write-concern unacknowledged by default"
    (let [db (m/create-connection! "mongodb://localhost:27017/db" :write-concern :unacknowledged)]
      (m/with-mongo db
        (m/delete! :coll {})
        (is (not (contains? (m/insert! :coll {:a 1}) :_id)))
        (is (= {:acknowledged false}
               (m/set! :coll {} {:a 2})))
        (m/with-write-concern :acknowledged
          (is (some? (:_id (m/insert! :coll {:a 1}))))
          (is (= {:acknowledged true
                  :matched-count 2
                  :modified-count 1}
                 (m/set! :coll {} {:a 2}))))))))

; TODO: Test retry-writes
;; (deftest retry-writes
;;   (is ())
;;   (let [db (m/create-connection! "mongodb://localhost:27017/db" :retry-writes? true)]
;;     (m/with-mongo db
;;       (m/transaction
;;         (m/insert! :coll {:a 1})
;;         (m/update! :coll {} {:$set {:a 2}})))))
