(ns com.timezynk.mongo.test.connection
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.config :as config]
   [com.timezynk.mongo.util :as u]))

(deftest check-uri
  (testing "uri is null"
    (is (thrown-with-msg? AssertionError
                          #""
                          (m/create-connection! nil)))))

(defn- db-name []
  (.getName config/*mongo-database*))

(deftest disjoint
  (testing "Switch to another database after exiting connection scope"
    (u/set-mongo-uri! "mongodb://localhost:27017/db-1")
    (u/wrap-mongo
     (is (= "db-1" (db-name))))
    (u/set-mongo-uri! "mongodb://localhost:27017/db-2")
    (u/wrap-mongo
     (is (= "db-2" (db-name))))))

(deftest nested
  (testing "Switch to another database inside the current connection scope"
    (u/set-mongo-uri! "mongodb://localhost:27017/db-1")
    (u/wrap-mongo
     (is (= "db-1" (db-name)))
     (u/set-mongo-uri! "mongodb://localhost:27017/db-2")
     (u/wrap-mongo
      (is (= "db-2" (db-name)))))))
