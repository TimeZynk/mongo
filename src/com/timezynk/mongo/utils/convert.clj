(ns com.timezynk.mongo.utils.convert
  (:require
   [clojure.core.reducers :as r])
  (:import [org.bson Document]))

(defn clj->doc
  "Convert a map or list of maps to BSON document."
  [v]
  (cond
    (map? v)
    (reduce (fn [d [k v]]
              (.append d (name k) (clj->doc v)))
            (Document.)
            v)
    (sequential? v)
    (->> (r/map clj->doc v)
         (into []))
    :else v))

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
         (into {}))
    :else v))

(defn list->doc
  "Convert a list of keywords to keys with value 1 in BSON document."
  [l]
  (->> (iterate identity 1)
       (interleave l)
       (apply assoc {})
       (clj->doc)))

(defn it->clj
  "Convert an iteration over BSON documents to list of maps."
  [result]
  (->> (.iterator result)
       (iterator-seq)
       (map doc->clj)))
