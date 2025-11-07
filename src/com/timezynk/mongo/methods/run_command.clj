(ns ^:no-doc com.timezynk.mongo.methods.run-command
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]])
  (:import [clojure.lang PersistentArrayMap]
           [java.util LinkedHashMap]))

(defn- build-params [cmd val options]
  (let [map (LinkedHashMap.)]
    (.put map cmd val)
    (when options
      (.putAll map options))
    (->bson map)))

(defmulti run-command-method
  (fn [_cmd _val _options]
    (some? *mongo-session*)))

(defmethod run-command-method true [cmd val options]
  (.runCommand *mongo-database*
               *mongo-session*
               (build-params cmd val options)
               PersistentArrayMap))

(defmethod run-command-method false [cmd val options]
  (.runCommand *mongo-database*
               (build-params cmd val options)
               PersistentArrayMap))
