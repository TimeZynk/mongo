(ns com.timezynk.mongo.test.utils.db-utils
  (:require
   [clojure.tools.logging :as log]
   [com.timezynk.mongo :as m]))

(defn clear-db []
  (try
    (doseq [coll (m/list-collection-names)]
      (m/drop-collection! coll))
    (catch Exception e
      (log/warn e "Failed to remove collections"))))

(defn test-case-db-fixture [f]
  (clear-db)
  (f))

(defn test-suite-db-fixture [f]
  (m/with-mongo "mongodb://127.0.0.1:27017/test"
    (f)))
