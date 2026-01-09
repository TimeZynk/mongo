(ns com.timezynk.mongo.files
  (:require
   [com.timezynk.mongo.assert :refer [assert-keys]]
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.convert :refer [file->clj]]
   [com.timezynk.mongo.file-methods.delete :refer [delete-method delete-by-id-method delete-by-query-method]]
   [com.timezynk.mongo.file-methods.download :refer [download-method download-by-id-method download-by-query-method download-array-method download-array-by-id-method]]
   [com.timezynk.mongo.file-methods.rename :refer [rename-method rename-by-id-method]]
   [com.timezynk.mongo.file-methods.upload :refer [upload-method]]
   [com.timezynk.mongo.helpers :refer [file-hooks get-filebucket]]
   [com.timezynk.mongo.methods.drop-collection :refer [drop-collection-method]]
   [com.timezynk.mongo.methods.fetch :refer [fetch-method]]))

; ------------------------
; Bucket
; ------------------------

(defn drop-bucket!
  {:added "1.0"}
  ([] (drop-bucket! nil))
  ([bucket] (drop-collection-method (get-filebucket ~bucket))))

; ------------------------
; File
; ------------------------

(defmacro info
  "Fetch files information.

   | Parameter    | Description
   | ---          | ---
   | `bucket`     | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `query`      | `map` A standard MongoDB query. Valid keys are:
   |              | `:chunk-size` Query on file chunk size.
   |              | `:filename` Query on file name.
   |              | `:length` Query on file length.
   |              | `:metadata` Query on file metadata.
   |              | `:upload-date` Query on file upload date. Default type is `java.util.Date`.
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
   `(->> (file-hooks
           (fetch-method (get-filebucket ~bucket) (->bson ~query) ~options))
         (map file->clj))))

(defmacro rename!
  "Rename file(s) by name.

   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `database-file` | `string` Name of file in database.
   | `filename`      | `string` New file name.

   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<database-file> <filename>]
               [<bucket> <database-file> <filename>])}
  ([database-file filename]        `(rename! nil ~database-file ~filename))
  ([bucket database-file filename] `(rename-method (get-filebucket ~bucket) ~database-file ~filename)))

(defmacro rename-by-id!
  "Rename file by id.

   | Parameter  | Description
   | ---        | ---
   | `bucket`   | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `id`       | `ObjectId` Id of file in database.
   | `filename` | `string` New file name.

   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<id> <filename>]
               [<bucket> <id> <filename>])}
  ([id filename]        `(rename-by-id! nil ~id ~filename))
  ([bucket id filename] `(rename-by-id-method (get-filebucket ~bucket) ~id ~filename)))

(defmacro download!
  "Download a single file from database.

   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `database-file` | `string` Name of file in database.
   | `output`        | `string/stream/nil` Disk file name or stream. If not set or `nil`, file will be stored to disk by its database name.
   | `:revision`     | `optional integer` File revision.

   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<database-file>]
               [<database-file> <output>]
               [<bucket> <database-file> <output> & :revision <integer>])}
  ([database-file] `(download! nil ~database-file nil))
  ([database-file output] `(download! nil ~database-file ~output))
  ([bucket database-file output & {:as options}]
   (assert-keys options #{:revision})
   `(download-method (get-filebucket ~bucket) ~database-file ~output ~options)))

(defmacro download-by-id!
  "Download a single file from database.

   | Parameter | Description
   | ---       | ---
   | `bucket`  | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `id`      | `ObjectId` Id of file in database.
   | `output`  | `string/stream/nil` Disk file name or stream. If not set or `nil`, file will be stored to disk by its database name.

   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<id>]
               [<id> <output>]
               [<bucket> <id> <output>])}
  ([id]               `(download-by-id! nil ~id nil))
  ([id output]        `(download-by-id! nil ~id ~output))
  ([bucket id output] `(download-by-id-method (get-filebucket ~bucket) ~id ~output)))

(defmacro download-by-query!
  "Download files from database.
   The database file name will be used as disk file name.

   | Parameter    | Description
   | ---          | ---
   | `bucket`     | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `query`      | `map` A standard MongoDB query. Valid keys are:
   |              | `:chunk-size` Query on file chunk size.
   |              | `:filename` Query on file name.
   |              | `:length` Query on file length.
   |              | `:metadata` Query on file metadata.
   |              | `:upload-date` Query on file upload date. Default type is `java.util.Date`.
   | `:collation` | `optional collation object` Collation used.
   | `:limit`     | `optional integer` Number of documents to fetch.
   | `:skip`      | `optional integer` Number of documents to skip before fetching.
   | `:sort`      | `optional map/list` A map/list of sorting criteria.

   **Returns**

   `nil`."
  {:added "1.0"
   :arglists '([<query>]
               [<bucket> <query> & :collation <collation object> :limit <count> :skip <count> :sort {}])}
  ([query] `(download-by-query! nil ~query))
  ([bucket query & {:as options}]
   (assert-keys options #{:collation :limit :skip :sort})
   `(file-hooks
      (download-by-query-method (get-filebucket ~bucket) (->bson ~query) ~options))))

(defmacro download-array!
  "Download a single file from database to a byte array.

   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `database-file` | `string` Name of file in database.
   | `:revision`     | `optional integer` File revision.

   **Returns**

   Byte array."
  {:added "1.0"
   :arglists '([<database-file>]
               [<bucket> <database-file> & :revision <integer>])}
  ([database-file] `(download-array! nil ~database-file))
  ([bucket database-file  & {:as options}]
   (assert-keys options #{:revision})
   `(download-array-method (get-filebucket ~bucket) ~database-file ~options)))

(defmacro download-array-by-id!
  "Download a single file from database to a byte array.

   | Parameter | Description
   | ---       | ---
   | `bucket`  | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `id`      | `ObjectId` Id of file in database.

   **Returns**

   Byte array."
  {:added "1.0"
   :arglists '([<id>]
               [<bucket> <id>])}
  ([id]        `(download-array-by-id! nil ~id))
  ([bucket id] `(download-array-by-id-method (get-filebucket ~bucket) ~id)))

(defmacro upload!
  "Upload file to database.

   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `input`         | `string/byte[]/stream` The input to store, either a file name, byte array, or input stream.
   | `database-file` | `string/nil` File name to store in database. If not set or `nil` use the input string.
   | `:chunk-size`   | `optional integer` Chunk size in bytes.
   | `:metadata`     | `optional map` File metadata.
   | `:prune?`       | `optional boolean` Remove previous revisions. Default false.

   **Returns**

   `ObjectId` of the created file."
  {:added "1.0"
   :arglists '([<input> <database-file>]
               [<bucket> <input> <database-file> & :chunk-size <integer> :metadata {}])}
  ([input] `(upload! nil ~input nil))
  ([input database-file] `(upload! nil ~input ~database-file))
  ([bucket input database-file & {:as options}]
   (assert-keys options #{:chunk-size :metadata :prune?})
   `(upload-method (get-filebucket ~bucket) ~input (or ~database-file ~input) ~options)))

(defmacro delete!
  "Delete files in database.
   The function makes two or more database calls, one to fetch files info and one for each file deletion. These calls are not atomic. Wrap the function in a transaction to make them atomic.

   | Parameter       | Description
   | ---             | ---
   | `bucket`        | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `database-file` | `string` File name.

   **Returns**

   A count of matching files.

   ```clojure
   {:deleted-count <number of matching files>}
   ```"
  {:added "1.0"
   :arglists '([<database-file>]
               [<bucket> <database-file>])}
  ([database-file]        `(delete! nil ~database-file))
  ([bucket database-file] `(delete-method (get-filebucket ~bucket) ~database-file)))

(defmacro delete-by-id!
  "Delete file in database.

   | Parameter | Description
   | ---       | ---
   | `bucket`  | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `id`      | `ObjectId` File id.

   **Returns**

   A count of matching files.

   ```clojure
   {:deleted-count 1}
   ```"
  {:added "1.0"
   :arglists '([<id>]
               [<bucket> <id>])}
  ([id]        `(delete-by-id! nil ~id))
  ([bucket id] `(delete-by-id-method (get-filebucket ~bucket) ~id)))

(defmacro delete-by-query!
  "Delete files in database.
   The function makes two or more database calls, one to fetch files info and one for each file deletion. These calls are not atomic. Wrap the function in a transaction to make them atomic.

   | Parameter    | Description
   | ---          | ---
   | `bucket`     | `keyword/string/nil` The bucket name. If not set or `nil` use the default database bucket.
   | `query`      | `map` A standard MongoDB query. Valid query keys are:
   |              | `:chunk-size` Query on file chunk size.
   |              | `:filename` Query on file name.
   |              | `:length` Query on file length.
   |              | `:metadata` Query on file metadata.
   |              | `:upload-date` Query on file upload date. Default type is `java.util.Date`.
   | `:collation` | `optional collation object` Collation used for query.
   | `:limit`     | `optional integer` Number of documents to fetch for query.
   | `:skip`      | `optional integer` Number of documents to skip before deleting for query.
   | `:sort`      | `optional map/list` A map/list of sorting criteria for query.

   **Returns**

   A count of matching files.

   ```clojure
   {:deleted-count <number of matching files>}
   ```"
  {:added "1.0"
   :arglists '([<query>]
               [<bucket> <query> & :collation <collation object> :limit <count> :skip <count> :sort {}])}
  ([query] `(delete-by-query! nil ~query))
  ([bucket query & {:as options}]
   (assert-keys options #{:collation :limit :skip :sort})
   `(delete-by-query-method (get-filebucket ~bucket) ~query ~options)))
