(ns ^:no-doc com.timezynk.mongo.convert
  (:import [clojure.lang PersistentArrayMap PersistentVector]))

(defn it->clj
  "Convert an iteration to list."
  [it]
  (-> (.iterator it)
      (iterator-seq)
      (or [])))

(defprotocol Convert
  (list->map [v]))

(extend-protocol Convert
  PersistentArrayMap
  (list->map [v] v)

  PersistentVector
  (list->map
  ;"Convert a list of keywords to keys with value 1 in BSON document."
    [l]
    (->> (repeat 1)
         (interleave l)
         (apply assoc {}))))
