(ns com.timezynk.mongo.test.watch-replace
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.watch :as w]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]
   [com.timezynk.mongo.test.utils.watch :as wu]
   [spy.core :as spy])
  (:import [com.mongodb MongoChangeStreamException]))

(use-fixtures :each #'dbu/test-suite-db-fixture #'dbu/test-case-db-fixture #'wu/with-callbacks)

(deftest collection-replace
  (testing "Catch replace"
    (m/create-collection! :coll)
    (w/on-replace :coll wu/on-watch)
    (testing "Full stream not allowed"
      (is (thrown-with-msg? MongoChangeStreamException
                            #""
                            (w/on-replace :coll wu/on-watch-2 :full? true))))
    (let [id (:_id (m/insert! :coll {:index 0}))]
      (m/replace-one! :coll {:_id id} {:index 1})
      (Thread/sleep 200)
      (is (spy/called-once? wu/on-watch))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ _ doc]]
                                (= {:_id id
                                    :index 1}
                                   doc)))))))

(deftest filter-replace
  (testing "Filter on field"
    (let [id (:_id (m/insert! :coll {:index 0}))]
      (w/on-replace :coll wu/on-watch :filter {:index {:$lt 2}})
      (m/replace-one! :coll {:_id id} {:index 1})
      (m/replace-one! :coll {:_id id} {:index 2})
      (Thread/sleep 200)
      (is (spy/called-once? wu/on-watch))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ _ doc]]
                                (= {:_id id
                                    :index 1}
                                   doc)))))))

(deftest full-replace
  (testing "Replace stream passes full documents"
    (m/create-collection! :coll :full-change? true)
    (let [id (:_id (m/insert! :coll {:index 0}))]
      (w/on-replace :coll wu/on-watch :full? true)
      (w/on-replace :coll wu/on-watch-2)
      (m/replace-one! :coll {:_id id} {:index 1})
      (Thread/sleep 200)
      (is (spy/called-once? wu/on-watch))
      (is (spy/called-once? wu/on-watch-2))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ _ new-doc old-doc]]
                                (and (= {:_id id
                                         :index 1}
                                        new-doc)
                                     (= {:_id id
                                         :index 0}
                                        old-doc)))))
      (testing "Create delta stream still works"
        (is (spy/call-matching? wu/on-watch-2
                                (fn [[_ _ doc]]
                                  (= {:_id id
                                      :index 1}
                                     doc))))))))
