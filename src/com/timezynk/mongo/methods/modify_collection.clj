(ns ^:no-doc com.timezynk.mongo.methods.modify-collection
  (:require
   [com.timezynk.mongo.utils.convert :as convert]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]])
  (:import
   [com.mongodb MongoNamespace]
   [com.mongodb.client.model CreateCollectionOptions ValidationLevel ValidationOptions]))

(defn modify-collection [coll {:keys [name collation level schema validation]}]
  (cond-> coll
    name   (.renameCollection (MongoNamespace. (.getName *mongo-database*)
                                               (name name)))
    collation (.collation collation)
    (or schema validation) (.validationOptions
                            (-> (ValidationOptions.)
                                (.validator (convert/clj->doc (merge (when schema
                                                                       {:$jsonSchema schema})
                                                                     (when validation
                                                                       validation))))
                                (.validationLevel (case (or level :strict)
                                                    :moderate ValidationLevel/MODERATE
                                                    :off      ValidationLevel/OFF
                                                    :strict   ValidationLevel/STRICT))))))
