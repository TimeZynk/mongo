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
  PersistentArrayMap
  (list->map [v] v) ; Already a map.

  PersistentVector
  (list->map [l]
    (->> (repeat 1)
         (interleave l)
         (apply assoc {}))))

(defn grid-fs-file->clj [^GridFSFile file]
  (let [codec   (map-codec (.getCodecRegistry *mongo-database*))
        context (.build (DecoderContext/builder))]
    {:chunk-size (.getChunkSize file)
     :file-name  (.getFilename  file)
     :_id        (.getObjectId  file)
     :length     (.getLength    file)
     :metadata
     (when-let [metadata (.getMetadata file)]
       (.decode codec
                (-> (BsonDocumentWrapper/asBsonDocument metadata
                                                        (CodecRegistries/fromCodecs [(DocumentCodec.)]))
                    (.asBsonReader))
                context))
     :upload-date
     (:a (.decode codec
                  (-> (.append (BsonDocument.)
                               "a"
                               (-> (.getUploadDate file)
                                   (.getTime)
                                   (BsonDateTime.)))
                      (.asBsonReader))
                  context))}))
