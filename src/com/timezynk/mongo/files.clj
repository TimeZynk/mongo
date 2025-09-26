(ns com.timezynk.mongo.files
  (:require
   [clojure.java.io :as io]
   [com.timezynk.mongo.assert :refer [assert-keys]]
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.convert :refer [file->clj it->clj]]
   [com.timezynk.mongo.file-methods.delete :refer [delete-method]]
   [com.timezynk.mongo.file-methods.download :refer [download-method]]
   [com.timezynk.mongo.file-methods.upload :refer [upload-method]]
   [com.timezynk.mongo.helpers :as h]
   [com.timezynk.mongo.methods.drop-collection :refer [drop-collection-method]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

; ------------------------
; Bucket
; ------------------------

(defn list-buckets [])

(defn modify-bucket! [])

(defn drop-bucket!
  {:added "1.0"}
  ([] (drop-bucket! nil))
  ([bucket] (drop-collection-method (h/get-filebucket ~bucket))))

; ------------------------
; File
; ------------------------

(defmacro info
  "Fetch files information.
   
   | Parameter    | Description
   | ---          | ---
   | `bucket`     | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `query`      | `map` A standard MongoDB query.
   | `:collation` | `optional collation object` Collation used.
   | `:limit`     | `optional integer` Number of documents to fetch.
   | `:skip`      | `optional integer` Number of documents to skip before fetching.
   | `:sort`      | `optional map/list` A map/list of sorting criteria.
   
   **Returns**

   A lazy sequence of matching documents."
  {:added "1.0"
   :arglists '([]
               [<query>]
               [<bucket> <query> & :collation <collation object> :limit <count> :skip <count> :sort {}])}
  ([] `(info {}))
  ([query] `(info nil ~query))
  ([bucket query & {:as options}]
   (assert-keys options #{:collation :limit :skip :sort})
   `(->> (fetch-method (h/get-filebucket ~bucket)
                       (->bson ~query)
                       ~options)
         (map file->clj)
         (it->clj))))

(defmacro info-one
  "Fetch information for a single file.
   
   | Parameter    | Description
   | ---          | ---
   | `bucket`     | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `query`      | `map` A standard MongoDB query.
   | `:collation` | `optional collation object` Collation used.
   | `:skip`      | `optional integer` Number of documents to skip before fetching.
   | `:sort`      | `optional map/list` A map/list of sorting criteria.
   
   **Returns**

   A lazy sequence of matching documents."
  {:added "1.0"
   :arglists '([]
               [<query>]
               [<bucket> <query> & :collation <collation object> :skip <count> :sort {}])}
  ([] `(info-one {}))
  ([query] `(info-one nil ~query))
  ([bucket query & {:as options}]
   (assert-keys options #{:collation :skip :sort})
   `(->> (fetch-method (h/get-filebucket ~bucket)
                       (->bson ~query)
                       (assoc ~options :limit 1))
         (it->clj)
         (map file->clj)
         (first))))

(defmacro download!
  "Download file to disk.
   
   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `database-file` | `string` File name in database.
   | `output-file`   | `string/nil` Disk file name. If not set or `nil` use database file name.
   | `:revision`     | `optional integer` File revision.
   
   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<database-file> <output-file>]
               [<bucket> <database-file> <output-file> & :revision <integer>])}
  ([database-file]             `(download! ~database-file ~database-file))
  ([database-file output-file] `(download! nil ~database-file ~output-file))
  ([bucket database-file output-file & {:as options}]
   (assert-keys options #{:revision})
   `(with-open [stream# (io/output-stream (or ~output-file
                                              ~database-file))]
      (download-method (h/get-filebucket ~bucket)
                       ~database-file
                       stream#
                       ~options))))

(defmacro fetch
  "Fetch file to memory.
   
   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `database-file` | `string` File name in database.
   | `:revision`     | `optional integer` File revision.
   
   **Returns**

   A `byte[]` with the file contents."
  {:added "1.0"
   :arglists '([<database-file>]
               [<bucket> <database-file> & :revision <integer>])}
  ([database-file] `(fetch nil ~database-file))
  ([bucket database-file & {:as options}]
   (assert-keys options #{:revision})
   `(let [stream# (ByteArrayOutputStream.)]
      (download-method (h/get-filebucket ~bucket)
                       ~database-file
                       stream#
                       ~options)
      (.toByteArray stream#))))

(defmacro upload!
  "Upload file from disk.
   
   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `input-file`    | `string` File name.
   | `database-file` | `string/nil` File name to store in database. If not set or `nil` use input file name.
   | `:metadata`     | `optional map` File metadata.
   
   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<input-file> <database-file>]
               [<bucket> <input-file> <database-file> & :metadata {}])}
  ([input-file database-file] `(upload! nil ~input-file ~database-file))
  ([bucket input-file database-file & {:as options}]
   (assert-keys options #{:metadata})
   `(with-open [stream# (io/input-stream ~input-file)]
      (upload-method (h/get-filebucket ~bucket)
                     (or ~database-file
                         ~input-file)
                     stream#
                     ~options))))

(defmacro insert!
  "Insert byte array as file.
   
   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `input`         | `byte[]` Byte array to store.
   | `database-file` | `string` File name to store in database.
   | `:metadata`     | `optional map` File metadata.
   
   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<input> <database-file>]
               [<bucket> <input> <database-file> & :metadata {}])}
  ([input database-file] `(insert! nil ~input ~database-file))
  ([bucket input database-file & {:as options}]
   (assert-keys options #{:metadata})
   `(let [stream# (ByteArrayInputStream. ~input)]
      (upload-method (h/get-filebucket ~bucket)
                     ~database-file
                     stream#
                     ~options))))

(defmacro delete!
  "Delete file in database.
   
   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `database-file` | `string` File name to store in database.
   
   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<database-file>]
               [<bucket> <database-file>])}
  ([database-file]        `(delete! nil ~database-file))
  ([bucket database-file] `(delete-method (h/get-filebucket ~bucket)
                                          ~database-file)))

; ------------------------
; Util
; ------------------------

(defn random-filename
  "Create a random file name."
  {:added "1.0"}
  []
  (apply str "File_" (repeatedly 20 #(rand-nth "abcdefghijklmnopqrstuvwxyz0123456789"))))
