(ns ^:no-doc com.timezynk.mongo.utils.convert
  (:require
   [clojure.core.reducers :as r]
   [com.timezynk.mongo.hooks :as hooks])
  (:import [java.util ArrayList]
           [org.bson BsonValue Document]))

;; Define your own methods to expand type conversion
(defmulti to-mongo (fn [v] (type v)))
(defmethod to-mongo :default [v] v)

(defn clj->doc
  "Convert a map or list of maps to BSON document."
  [v]
  (cond
    (keyword? v)
    (let [v-ns (namespace v)]
      (str v-ns (when v-ns "/") (name v)))
    (map? v)
    (->> (hooks/*write-hook* v)
         (reduce (fn [d [k v]]
                   (.append d (clj->doc k) (clj->doc v)))
                 (Document.)))
    (coll? v)
    (mapv clj->doc v)
    :else (to-mongo v)))

(defn doc->clj
  "Convert a BSON document to map."
  [v]
  (cond
    (= (type v) Document)
    (->> (.entrySet v)
         (.toArray)
         (r/map (fn [x]
                  [(keyword (.getKey x))
                   (doc->clj (.getValue x))]))
         (into {})
         (hooks/*read-hook*))
    (instance? BsonValue v)
    (.getValue v)
    (= (type v) ArrayList)
    (mapv doc->clj v)
    :else v))

(defn list->doc
  "Convert a list of keywords to keys with value 1 in BSON document."
  [l]
  (->> (repeat 1)
       (interleave l)
       (apply assoc {})
       (clj->doc)))

(defn it->clj
  "Convert an iteration over BSON documents to list of maps."
  [result]
  (->> (.iterator result)
       (iterator-seq)
       (map doc->clj)))
