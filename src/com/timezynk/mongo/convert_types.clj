(ns com.timezynk.mongo.convert-types
  "Type conversion between Clojure and MongoDB.
   
   * `clj->doc` converts from Clojure to MongoDB,
   * `doc->clj` converts from MongoDB to Clojure."
  (:require
   [clojure.core.reducers :as r]
   [com.timezynk.mongo.hooks :as hooks])
  (:import [clojure.lang Keyword]
           [java.util Collection List Map]
           [org.bson BsonValue Document]))

(defprotocol ConvertTypes
  (clj->doc [v] "Convert types from Clojure to MongoDB.")
  (doc->clj [v] "Convert types from MongoDB to Clojure."))

(extend-protocol ConvertTypes
  Keyword
  (clj->doc [v]
    (let [v-ns (namespace v)]
      (str v-ns (when v-ns "/") (name v))))

  Collection
  (clj->doc [v]
    (mapv clj->doc v))
  (doc->clj [v]
    (mapv doc->clj v))

  List
  (clj->doc [v]
    (mapv clj->doc v))
  (doc->clj [v]
    (mapv doc->clj v))

  Map
  (clj->doc [v]
    (->> (hooks/*write-hook* v)
         (reduce (fn [d [k v]]
                   (.append d (clj->doc k) (clj->doc v)))
                 (Document.))))

  Document
  (doc->clj [v]
    (->> (.entrySet v)
         (.toArray)
         (r/map (fn [x]
                  [(keyword (.getKey x))
                   (doc->clj (.getValue x))]))
         (into {})
         (hooks/*read-hook*)))

  BsonValue
  (doc->clj [v]
    (.getValue v))

  Object
  (clj->doc [v] v)
  (doc->clj [v] v)

  nil
  (clj->doc [_] nil)
  (doc->clj [_] nil))

(defn ^:no-doc list->doc
  "Convert a list of keywords to keys with value 1 in BSON document."
  [l]
  (->> (repeat 1)
       (interleave l)
       (apply assoc {})
       (clj->doc)))

(defn ^:no-doc it->clj
  "Convert an iteration over BSON documents to list of maps."
  [result]
  (->> (.iterator result)
       (iterator-seq)
       (map doc->clj)))
