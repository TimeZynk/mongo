(ns com.timezynk.mongo.test.files
  (:require
   [clojure.java.io :refer [delete-file]]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.files :as mf]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [java.util Date]
           [org.bson.codecs Codec]
           [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(defn datetime-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readDateTime reader))

    (encode [_ _ _ _]
      ())

    (getEncoderClass [_this]
      Date)))

(deftest upload-download
  (let [file-name "temp.bin"]
    (try
      (let [file    "ABCDEFGHIJ"
            _       (spit file-name file)
            db-file (mf/random-filename)
            id      (mf/upload! nil file-name db-file)]
        (is (= ObjectId (type id)))
        (delete-file file-name)
        (testing "Download by file-name"
          (mf/download! nil db-file file-name)
          (is (= file (slurp file-name)))))
      (finally
        (delete-file file-name :silently)))))

(deftest file-info
  (let [str-1   "ABCDEFGHIJ"
        str-2   "KLMNOPQRST"
        id      (mf/insert! :bucket (.getBytes str-1) "file-1" :metadata {:a "Hey"})
        info    (mf/info-one :bucket {})]
    (testing "Upload a byte array and fetch the file info"
      (is (= {:chunk-size 261120
              :file-name  "file-1"
              :_id        id
              :length     10
              :metadata   {:a "Hey"}}
             (dissoc info :upload-date))))
    (testing "Check that default datetime decoder is used"
      (is (= Date (-> info :upload-date type))))
    (testing "Use a different datetime decoder"
      (m/with-codecs [(datetime-codec)]
                     {}
        (let [info (mf/info-one :bucket {})]
          (is (= Long (-> info :upload-date type))))))))
