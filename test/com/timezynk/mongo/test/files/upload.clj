(ns com.timezynk.mongo.test.files.upload
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.files :as mf]
   [com.timezynk.mongo.hooks :as mh]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(def ^:private ^:const bucket       "bucket")
(def ^:private ^:const f-name       "temp.bin")
(def ^:private ^:const db-name      "db-name")
(def ^:private ^:const file-content "ABCDEFGHIJ")

(defn- check-file [id]
  (let [file  (m/fetch-one :bucket.files {:_id id})
        chunk (m/fetch-one :bucket.chunks {:files_id id})]
    (testing "Check result"
      (is (= ObjectId (type id))))
    (testing "Check .files content"
      (is (= db-name (:filename file)))
      (is (= 10 (:length file))))
    (testing "Check .chunks content"
      (is (= 0 (:n chunk)))
      (is (= file-content
             (->> (:data chunk)
                  (map char)
                  (apply str)))))))

(deftest upload-file
  (try
    (spit f-name file-content)
    (check-file (mf/upload! bucket f-name db-name))
    (finally
      (io/delete-file f-name :silently))))

(deftest upload-array
  (check-file (mf/upload! bucket
                          (.getBytes file-content)
                          db-name)))

(deftest upload-stream
  (check-file (mf/upload! bucket
                          (io/input-stream (.getBytes file-content))
                          db-name)))

(deftest prune
  (mf/upload! bucket (.getBytes "ABC") db-name :metadata {:a 1})
  (mf/upload! bucket (.getBytes "DEF") db-name :metadata {:a 2})
  (is (= #{{:a 1} {:a 2}}
         (->> (mf/info bucket {})
              (map :metadata)
              (set))))
  (mf/upload! bucket (.getBytes "DEF") db-name :metadata {:a 3} :prune? true)
  (is (= [{:a 3}]
         (->> (mf/info bucket {})
              (map :metadata)))))

(deftest upload-with-id
  (let [id (ObjectId.)]
    (mh/with-hooks {:read #(set/rename-keys % {:_id :id})}
      (is (nil? (mf/upload! nil (.getBytes "ABC") db-name :_id id)))
      (is (= id
             (-> (mf/info)
                 (first)
                 (:id)))))))
