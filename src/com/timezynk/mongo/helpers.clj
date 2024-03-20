(ns ^:no-doc com.timezynk.mongo.helpers
  (:require
   [com.timezynk.mongo.config :refer [*mongo-database*]]
   [com.timezynk.mongo.convert-types :refer [clj->doc doc->clj it->clj]]
   [com.timezynk.mongo.guards :refer [*insert-guard* *update-guard* catch-return]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]
   [com.timezynk.mongo.methods.fetch-and-update :refer [fetch-and-update-method fetch-and-update-options]]
   [com.timezynk.mongo.methods.insert :refer [insert-method insert-options]]
   [com.timezynk.mongo.methods.update :refer [update-method update-one-method update-options]])
  (:import [com.mongodb.client MongoCollection]))

(defmacro ^:no-doc get-collection ^MongoCollection [coll]
  `(.getCollection *mongo-database*
                   (name ~coll)))

(defmacro ^:no-doc do-fetch [coll query options]
  `{:pre [~coll ~query]}
  `(-> (fetch-method (get-collection ~coll)
                     (clj->doc ~query)
                     ~options)
       (it->clj)))

(defmacro ^:no-doc do-insert [coll docs options]
  `{:pre [~coll]}
  `(catch-return
    (*insert-guard* ~docs)
    (let [docs# (clj->doc ~docs)]
      (-> (get-collection ~coll)
          (insert-options ~options)
          (insert-method docs#))
      (doc->clj docs#))))

(defmacro ^:no-doc do-update [coll query update options]
  `{:pre [~coll ~query]}
  `(catch-return
    (*update-guard* ~update)
    (let [result# (update-method (get-collection ~coll)
                                 (clj->doc ~query)
                                 (clj->doc ~update)
                                 (update-options ~options))]
      {:matched-count  (.getMatchedCount result#)
       :modified-count (.getModifiedCount result#)})))

(defmacro ^:no-doc do-update-one [coll query update options]
  `{:pre [~coll ~query]}
  `(catch-return
    (*update-guard* ~update)
    (let [result# (update-one-method (get-collection ~coll)
                                     (clj->doc ~query)
                                     (clj->doc ~update)
                                     (update-options ~options))]
      {:matched-count  (.getMatchedCount result#)
       :modified-count (.getModifiedCount result#)})))

(defmacro ^:no-doc do-fetch-and-update-one [coll query update options]
  `{:pre [~coll ~query]}
  `(catch-return
    (*update-guard* ~update)
    (-> (fetch-and-update-method (get-collection ~coll)
                                 (clj->doc ~query)
                                 (clj->doc ~update)
                                 (fetch-and-update-options ~options))
        (doc->clj))))
