(ns ^:no-doc com.timezynk.mongo.methods.distinct
  (:require
   [clojure.string :as str]
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.convert :refer [it->clj]]
   [com.timezynk.mongo.helpers :refer [get-collection]]
   [com.timezynk.mongo.methods.list-collections :refer [list-collections-method]])
  (:import [com.timezynk.mongo.codecs.distinct DistinctCodec]))

(defn- validate [coll field validate?]
  (and validate?
       (not (-> (filter #(= (name coll) (:name %))
                        (list-collections-method))
                (first)
                (get-in (concat [:options :validator :$jsonSchema :properties]
                                (->> (str/split (name field) #"\.")
                                     (map keyword)
                                     (interpose :properties))))))
       (throw (IllegalArgumentException. (str "Not part of schema: " field)))))

(defmulti distinct-method
  (fn [_coll _field _query _options]
    (some? *mongo-session*)))

(defmethod distinct-method true [coll field query {:keys [validate?]}]
  (validate coll field validate?)
  (-> (.distinct (get-collection coll)
                 *mongo-session*
                 field
                 (->bson query)
                 DistinctCodec)
      (it->clj)))

(defmethod distinct-method false [coll field query {:keys [validate?]}]
  (validate coll field validate?)
  (-> (.distinct (get-collection coll)
                 field
                 (->bson query)
                 DistinctCodec)
      (it->clj)))
