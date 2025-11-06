(ns ^:no-doc com.timezynk.mongo.methods.replace
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-session*]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.methods.update :refer [get-options update-result]]
   [com.timezynk.mongo.padding :refer [*replace-padding*]])
  (:import [com.mongodb.client.model ReplaceOptions]
           [com.mongodb.client.result
            UpdateResult]))

(defn- replace-options ^ReplaceOptions [options]
  (get-options (ReplaceOptions.) options))

(defmulti replace-method ^UpdateResult
  (fn [_coll _query _doc _options]
    (some? *mongo-session*)))

(defmethod replace-method true [coll query doc options]
  (-> (.replaceOne (h/get-collection coll)
                   *mongo-session*
                   (->bson query)
                   (*replace-padding* doc)
                   (replace-options options))
      (update-result)))

(defmethod replace-method false [coll query doc options]
  (-> (.replaceOne (h/get-collection coll)
                   (->bson query)
                   (*replace-padding* doc)
                   (replace-options options))
      (update-result)))
