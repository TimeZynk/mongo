(ns com.timezynk.mongo.test.files.info
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.files :as mf]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [java.util Date]
           [org.bson.codecs Codec]))

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

(deftest file-info
  (let [id    (mf/upload! :bucket (.getBytes "ABCDEFGHIJ") "file-1" :metadata {:a "Hey"})
        info  (first (mf/info :bucket {:length 10}))]
    (testing "Upload a byte array and fetch the file info"
      (is (= {:chunk-size 261120
              :filename   "file-1"
              :_id        id
              :length     10
              :metadata   {:a "Hey"}}
             (dissoc info :upload-date))))
    (testing "Check that default datetime decoder is used"
      (is (= Date (-> info :upload-date type))))
    (testing "Use a different datetime decoder"
      (m/with-codecs [(datetime-codec)]
                     {}
        (let [info (first (mf/info :bucket {}))]
          (is (= Long (-> info :upload-date type))))))))
