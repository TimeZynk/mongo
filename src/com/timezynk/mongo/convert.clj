(ns ^:no-doc com.timezynk.mongo.convert
  (:require
   [com.timezynk.mongo.codecs.map :refer [map-codec]]
   [com.timezynk.mongo.config :refer [*mongo-database*]])
  (:import [clojure.lang PersistentArrayMap PersistentVector]
           [com.mongodb.client.gridfs.model GridFSFile]
           [org.bson BsonDateTime BsonDocument BsonDocumentWrapper]
           [org.bson.codecs DocumentCodec DecoderContext]
           [org.bson.codecs.configuration CodecRegistries]))

(defn it->clj
  "Convert an iteration to list."
  [it]
  (-> (.iterator it)
      (iterator-seq)
      (or [])))

(defprotocol Convert
  "Convert a list of keywords to keys with value 1 in BSON document."
  (list->map [v]))

(extend-protocol Convert
  PersistentVector
  (list->map [v]
    (->> (repeat 1)
         (interleave v)
         (apply assoc {})))

  PersistentArrayMap
  (list->map [v] v))

(defn decode-bson-document [doc]
  (let [codec   (map-codec (.getCodecRegistry *mongo-database*))
        context (.build (DecoderContext/builder))]
    (.decode codec
             (.asBsonReader doc)
             context)))

(defn decode-document [doc]
  (->> (CodecRegistries/fromCodecs [(DocumentCodec.)])
       (BsonDocumentWrapper/asBsonDocument doc)
       (decode-bson-document)))

(defn decode-bson-value [v]
  (-> (.append (BsonDocument.)
               "v" v)
      (decode-bson-document)
      (:v)))

(defn file->clj [^GridFSFile file]
  {:chunk-size (.getChunkSize file)
   :file-name  (.getFilename  file)
   :_id        (.getObjectId  file)
   :length     (.getLength    file)
   :metadata   (when-let [metadata (.getMetadata file)]
                 (decode-document metadata))
   :upload-date (-> (.getUploadDate file)
                    (.getTime)
                    (BsonDateTime.)
                    (decode-bson-value))})
