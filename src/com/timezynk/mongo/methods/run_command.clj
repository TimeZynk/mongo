(ns com.timezynk.mongo.methods.run-command
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]])
  (:import [clojure.lang PersistentArrayMap]))

(defmulti run-command-method
  (fn [_cmd] (some? *mongo-session*)))

(defmethod run-command-method true [cmd]
  (.runCommand *mongo-database*
               *mongo-session*
               (->bson cmd)
               PersistentArrayMap))

(defmethod run-command-method false [cmd]
  (.runCommand *mongo-database*
               (->bson cmd)
               PersistentArrayMap))
