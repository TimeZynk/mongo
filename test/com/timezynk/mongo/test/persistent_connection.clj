(ns com.timezynk.mongo.test.persistent-connection
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.timezynk.mongo.config :as config]
   [com.timezynk.mongo.util :as u]))

(defn- db-name []
  (.getName config/*mongo-database*))

(deftest disjoint
  (testing "Switch to another server after exiting connection scope"
    (u/set-mongo-uri! "mongodb://localhost:27017/db-1")
    (u/wrap-mongo
      (is (= "db-1" (db-name))))
    (u/set-mongo-uri! "mongodb://localhost:27017/db-2")
    (u/wrap-mongo
      (is (= "db-2" (db-name))))))

(deftest nested
  (testing "Switch to another server inside the current connection scope"
    (u/set-mongo-uri! "mongodb://localhost:27017/db-1")
    (u/wrap-mongo
      (is (= "db-1" (db-name)))
      (u/set-mongo-uri! "mongodb://localhost:27017/db-2")
      (u/wrap-mongo
        (is (= "db-2" (db-name)))))))
