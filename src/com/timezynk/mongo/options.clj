(ns ^:no-doc com.timezynk.mongo.options
  (:require
   [com.timezynk.mongo.utils.convert :as convert]))

(defn apply-options [result {:keys [collation limit only skip sort]}]
  (cond-> result
    collation (.collation collation)
    limit     (.limit limit)
    only      (.projection (convert/clj->doc only))
    skip      (.skip skip)
    sort      (.sort (convert/clj->doc sort))))
