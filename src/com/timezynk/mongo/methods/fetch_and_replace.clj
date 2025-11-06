(ns ^:no-doc com.timezynk.mongo.methods.fetch-and-replace
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.methods.fetch-and-update :refer [get-options]]
   [com.timezynk.mongo.padding :refer [*replace-padding*]])
  (:import [org.bson Document]
           [com.mongodb.client.model FindOneAndReplaceOptions]))

(defn- fetch-and-replace-options ^FindOneAndReplaceOptions [options]
  (get-options (FindOneAndReplaceOptions.) options))

(defmulti fetch-and-replace-method ^Document
  (fn [_coll _query _doc _options]
    (some? *mongo-session*)))

(defmethod fetch-and-replace-method true [coll query doc options]
  (.findOneAndReplace (h/get-collection coll)
                      *mongo-session*
                      (->bson query)
                      (*replace-padding* doc)
                      (fetch-and-replace-options options)))

(defmethod fetch-and-replace-method false [coll query doc options]
  (.findOneAndReplace (h/get-collection coll)
                      (->bson query)
                      (*replace-padding* doc)
                      (fetch-and-replace-options options)))
