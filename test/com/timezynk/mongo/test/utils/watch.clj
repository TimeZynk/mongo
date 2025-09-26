(ns com.timezynk.mongo.test.utils.watch
  (:require
   [spy.core :as spy]))

(defn on-watch [] ())

(defn on-watch-2 [] ())

(defn with-callbacks [f]
  (with-redefs [on-watch   (spy/stub)
                on-watch-2 (spy/stub)]
    (f)))
