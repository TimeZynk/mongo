(ns ^:no-doc com.timezynk.mongo.methods.modify-collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]])
  (:import [com.mongodb MongoNamespace]))

(defn- rename-collection [coll name]
  (if *mongo-session*
    (.renameCollection coll *mongo-session* (MongoNamespace. (.getName *mongo-database*)
                                                             (clojure.core/name name)))
    (.renameCollection coll (MongoNamespace. (.getName *mongo-database*)
                                             (clojure.core/name name)))))

(defn modify-collection [coll {:keys [name]}]
  (cond-> coll
    name (rename-collection name)))
