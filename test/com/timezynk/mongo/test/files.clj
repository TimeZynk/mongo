(ns com.timezynk.mongo.test.files
  (:require
   [clojure.java.io :refer [delete-file]]
   [clojure.test :refer [are deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
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

(deftest file
  (let [file-name "temp.bin"]
    (try
      (let [file "ABCDEFGHIJ"
            _    (spit file-name file)
            id   (m/upload-file! nil file-name :metadata {:a "Hey"})
            _    (is (= ObjectId (type id)))
            info (first (m/file-info))]
        (testing "Upload a file and fetch the file info"
          (are [a b] (= a (b info))
            261120     :chunk-size
            file-name  :file-name
            id         :_id
            10         :length
            {:a "Hey"} :metadata))
        (testing "Check that default datetime decoder is used"
          (is (= Date (-> info :upload-date type))))
        (testing "Use a different datetime decoder"
          (m/with-codecs [(datetime-codec)]
                         {}
            (let [info (first (m/file-info))]
              (is (= Long (-> info :upload-date type))))))
        (delete-file file-name)
        (testing "Download by file-name"
          (m/download-file nil file-name)
          (is (= file (slurp file-name)))))
      (finally
        (delete-file file-name :silently)))))
