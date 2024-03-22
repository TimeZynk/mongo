(ns ^:no-doc com.timezynk.mongo.methods.create-collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]]
   [com.timezynk.mongo.convert-types :refer [clj->doc]]
   [com.timezynk.mongo.schema :refer [convert-schema]])
  (:import
   [com.mongodb.client.model CreateCollectionOptions ValidationAction ValidationLevel ValidationOptions]))

(defn collection-options [{:keys [collation level schema validation]}]
  (-> (cond-> (CreateCollectionOptions.)
        collation (.collation collation))
      (.validationOptions (-> (ValidationOptions.)
                              (.validator (clj->doc (merge (convert-schema schema)
                                                           validation)))
                              (.validationLevel (case (or level :strict)
                                                  :moderate ValidationLevel/MODERATE
                                                  :off      ValidationLevel/OFF
                                                  :strict   ValidationLevel/STRICT))
                              (.validationAction ValidationAction/ERROR)))))

(defmulti create-collection-method
  (fn [_name _options]
    (some? *mongo-session*)))

(defmethod create-collection-method true [coll options]
  (.createCollection *mongo-database* *mongo-session* coll options))

(defmethod create-collection-method false [coll options]
  (.createCollection *mongo-database* coll options))
