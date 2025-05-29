(ns com.timezynk.mongo.test.watch-insert
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.watch :as w]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]
   [com.timezynk.mongo.test.utils.watch :as wu]
   [spy.core :as spy])
  (:import [java.util Date]
           [org.bson.codecs Codec]))

(use-fixtures :each #'dbu/test-suite-db-fixture #'dbu/test-case-db-fixture #'wu/with-callbacks)

(deftest collection-insert
  (testing "Listen for inserts, ignore the update"
    (m/create-collection! :coll)
    (w/on-insert :coll wu/on-watch)
    (let [id-1 (:_id (m/insert! :coll {:name "Name1"}))
          id-2 (:_id (m/insert! :coll {:name "Name2"}))]
      (m/set-one! :coll {:name "Name1"} {:name "Name3"})
      (Thread/sleep 200)
      (is (spy/called-n-times? wu/on-watch 2))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ time doc]]
                                (and (= Date (type time))
                                     (= {:_id id-1
                                         :name "Name1"}
                                        doc)))))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ time {:keys [_id name]}]]
                                (and (= Date (type time))
                                     (= id-2 _id)
                                     (= "Name2" name))))))))

(defn datetime-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readDateTime reader))

    (encode [_ _ _ _]
      ())

    (getEncoderClass [_this]
      Date)))

(deftest decode-time
  (testing "Get date-time in different format"
    (m/create-collection! :coll)
    (m/with-codecs [(datetime-codec)]
                   {}
      (w/on-insert :coll wu/on-watch))
    (m/insert! :coll {:name "Name1"})
    (Thread/sleep 200)
    (is (spy/called-once? wu/on-watch))
    (is (spy/call-matching? wu/on-watch
                            (fn [[_ time _]]
                              (= Long (type time)))))))

(deftest collation
  (m/create-collection! :coll)
  (w/on-insert :coll wu/on-watch :filter {:num "12"})
  (w/on-insert :coll wu/on-watch-2 :filter {:num "12"} :collation (m/collation "se" :alternate :shifted))
  (m/insert! :coll {:num "12"})
  (m/insert! :coll {:num ".12 "})
  (Thread/sleep 200)
  (testing "Exact match without collation"
    (is (spy/called-once? wu/on-watch)))
  (testing "Match with collation"
    (is (spy/called-n-times? wu/on-watch-2 2))))

(deftest database-insert
  (testing "Listen for all inserts"
    (w/on-insert nil wu/on-watch)
    (let [id-1 (:_id (m/insert! :coll-1 {:name "Name1"}))
          id-2 (:_id (m/insert! :coll-2 {:name "Name2"}))]
      (Thread/sleep 200)
      (is (spy/called-n-times? wu/on-watch 2))
      (is (spy/call-matching? wu/on-watch
                              (fn [[coll time {:keys [_id name]}]]
                                (and (= :coll-1 coll)
                                     (= Date (type time))
                                     (= id-1 _id)
                                     (= "Name1" name)))))
      (is (spy/call-matching? wu/on-watch
                              (fn [[coll time {:keys [_id name]}]]
                                (and (= :coll-2 coll)
                                     (= Date (type time))
                                     (= id-2 _id)
                                     (= "Name2" name))))))))

(deftest filter-insert
  (testing "Filter inserts"
    (w/on-insert nil wu/on-watch :filter {:name "Name1"})
    (m/insert! :coll-1 {:name "Name1"})
    (m/insert! :coll-2 {:name "Name2"})
    (Thread/sleep 200)
    (is (spy/called-once? wu/on-watch))
    (is (spy/call-matching? wu/on-watch
                            (fn [[coll _ {:keys [name]}]]
                              (and (= :coll-1 coll)
                                   (= "Name1" name)))))))
