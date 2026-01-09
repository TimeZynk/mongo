(ns com.timezynk.mongo.test.files.download
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo.files :as mf]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]
   [com.timezynk.mongo.util :as mu])
  (:import [com.mongodb MongoGridFSException]
           [java.io ByteArrayOutputStream FileNotFoundException]
           [java.lang AssertionError]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest check-string
  (is (thrown? AssertionError
               (mf/upload! (.getBytes "A")))))

(deftest download-file
  (mf/upload! (.getBytes "A") "file-1")
  (try
    (mf/download! "file-1" "temp-file")
    (is (= "A"
           (slurp "temp-file")))
    (finally
      (io/delete-file "temp-file" :silently))))

(deftest download-by-id
  (let [id (mf/upload! (.getBytes "A") "temp-file-1")]
    (try
      (mf/download-by-id! id)
      (is (= "A"
             (slurp "temp-file-1")))
      (finally
        (io/delete-file "temp-file-1" :silently)))))

(deftest download-to-stream
  (mf/upload! (.getBytes "A") "file-1")
  (is (= "A"
         (with-open [stream (ByteArrayOutputStream.)]
           (mf/download! "file-1" stream)
           (mu/->string (.toByteArray stream))))))

(deftest revision
  (mf/upload! (.getBytes "A") "file-1")
  (mf/upload! (.getBytes "B") "file-1")
  (testing "Latest revision"
    (is (= "B" (mu/->string (mf/download-array! "file-1")))))
  (testing "First revision"
    (is (= "A" (mu/->string (mf/download-array! nil "file-1" :revision 0)))))
  (testing "Second revision"
    (is (= "B" (mu/->string (mf/download-array! nil "file-1" :revision 1)))))
  (try
    (testing "Revision doesn't exist"
      (is (thrown? MongoGridFSException
                   (mf/download! nil "file-1" nil :revision 2))))
    (testing "File was created but is empty"
      (is (= "" (slurp "file-1"))))
    (finally
      (io/delete-file "file-1" :silently))))

(deftest download-by-query
  (mf/upload! (.getBytes "A") "temp-file-1")
  (mf/upload! (.getBytes "B") "temp-file-1")
  (mf/upload! (.getBytes "C") "temp-file-2")
  (mf/upload! (.getBytes "DE") "temp-file-3")
  (try
    (mf/download-by-query! {:length 1})
    (is (= "B" (slurp "temp-file-1")))
    (is (= "C" (slurp "temp-file-2")))
    (is (thrown? FileNotFoundException
                 (slurp "temp-file-3")))
    (finally
      (io/delete-file "temp-file-1" :silently)
      (io/delete-file "temp-file-2" :silently)
      (io/delete-file "temp-file-3" :silently))))
