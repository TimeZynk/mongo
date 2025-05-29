(ns com.timezynk.mongo.files
  (:require
   [clojure.java.io :as io]
   [com.timezynk.mongo.assert :refer [assert-keys]]
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.convert :refer [file->clj it->clj]]
   [com.timezynk.mongo.file-methods.delete :refer [delete-method]]
   [com.timezynk.mongo.file-methods.download :refer [download-method]]
   [com.timezynk.mongo.file-methods.info :refer [info-method]]
   [com.timezynk.mongo.file-methods.upload :refer [upload-method]]
   [com.timezynk.mongo.helpers :as h]))

(defmacro info
  "Fetch files.
   
   | Parameter    | Description
   | ---          | ---
   | `bucket`     | `keyword/string/nil` The bucket name. Can be nil to get the default database bucket.
   | `query`      | `map` A standard MongoDB query.
   | `:collation` | `optional collation object` Collation used.
   | `:limit`     | `optional integer` Number of documents to fetch.
   | `:skip`      | `optional integer` Number of documents to skip before fetching.
   | `:sort`      | `optional map/list` A map/list of sorting criteria.
   
   **Returns**

   A lazy sequence of matching documents."
  {:added "1.0"
   :arglists '([]
               [<bucket>]
               [<bucket> <query> & :collation <collation object> :limit <count> :only {} :skip <count> :sort {}])}
  ([] `(info nil {}))
  ([bucket] `(info ~bucket {}))
  ([bucket query & {:as options}]
   (assert-keys options #{:collation :limit :skip :sort})
   `(->> (info-method (h/get-filebucket ~bucket)
                      (->bson ~query)
                      ~options)
         (it->clj)
         (map file->clj))))

(defmacro download!
  {:added "1.0"}
  [bucket file & {:as options}]
  (assert-keys options #{:revision})
  `(with-open [stream# (io/output-stream ~file)]
     (download-method (h/get-filebucket ~bucket)
                      ~file
                      stream#
                      ~options)))

(defmacro upload!
  {:added "1.0"}
  [bucket file & {:as options}]
  (assert-keys options #{:metadata})
  `(with-open [stream# (io/input-stream ~file)]
     (upload-method (h/get-filebucket ~bucket)
                    (if (= String (type ~file))
                      ~file
                      (apply str "File_" (repeatedly 20 #(rand-nth "abcdefghijklmnopqrstuvwxyz0123456789"))))
                    stream#
                    ~options)))

(defmacro delete!
  {:added "1.0"}
  ([id]        `(delete! nil ~id))
  ([bucket id] `(delete-method (h/get-filebucket ~bucket)
                               ~id)))

;; (defn modify-bucket! [])

;; (defn drop-bucket! [])
