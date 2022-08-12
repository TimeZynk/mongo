(ns com.timezynk.mongo.test.utils.db-utils
  (:require
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]))

(defn clear-db []
  (try
    (doseq [coll (mongo/list-collection-names)]
      (mongo/drop-collection! coll))
    (catch Exception e
      (log/warn e "Failed to remove collections"))))

(defn test-case-db-fixture [f]
  (try
    (f)
    (finally
      (clear-db))))

(defn test-suite-db-fixture [f]
  (mongo/with-mongo "mongodb://127.0.0.1:27017/test"
    (f)))
