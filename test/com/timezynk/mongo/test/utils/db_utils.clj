(ns com.timezynk.mongo.test.utils.db-utils
  (:require
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]))

(defn clear-db []
  (try
    (mongo/with-mongo "mongodb://127.0.0.1:27017" "test"
      (doseq [coll (mongo/get-collections)]
        (mongo/drop-collection! coll)))
    (catch Exception e
      (log/warn e "Failed to remove collections"))))

(defn empty-db []
  (try
    (doseq [coll (mongo/get-collections)]
      (when-not (= "collection.headers" coll)
        (mongo/delete! coll {})))
    (catch Exception e
      (log/warn e "Failed to delete documents"))))

(defn test-case-db-fixture [f]
  (f)
  (empty-db))

(defn test-suite-db-fixture [f]
  (mongo/with-mongo "mongodb://127.0.0.1:27017" "test"
    (f))
  (clear-db))
