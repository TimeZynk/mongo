(ns ^:no-doc com.timezynk.mongo.methods.modify-collection
  (:require
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]]
   [com.timezynk.mongo.schema :refer [convert-schema]]
   [com.timezynk.mongo.utils.convert :as convert])
  (:import [com.mongodb MongoNamespace]
           [org.bson Document]))

(defn- set-name [name]
  (MongoNamespace. (.getName *mongo-database*)
                   (clojure.core/name name)))

(defmulti rename-collection-method
  (fn [_coll _name] (some? *mongo-session*)))

(defmethod rename-collection-method true [coll name]
  (.renameCollection coll *mongo-session* (set-name name)))

(defmethod rename-collection-method false [coll name]
  (.renameCollection coll (set-name name)))

(defn- set-schema [coll schema]
  (-> (Document.)
      (.append "collMod" (-> coll .getNamespace .getCollectionName))
      (.append "validator" (-> schema convert-schema convert/clj->doc))))

(defmulti set-schema-method
  (fn [_coll _schema] (some? *mongo-session*)))

(defmethod set-schema-method true [coll schema]
  (.runCommand *mongo-database* *mongo-session* (set-schema coll schema)))

(defmethod set-schema-method false [coll schema]
  (.runCommand *mongo-database* (set-schema coll schema)))

(defn modify-collection [coll {:keys [name schema]}]
  (cond-> coll
    name (rename-collection-method name)
    schema (set-schema-method schema)))
