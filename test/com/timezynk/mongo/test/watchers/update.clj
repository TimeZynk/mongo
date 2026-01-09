(ns com.timezynk.mongo.test.watchers.update
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.watch :as w]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]
   [com.timezynk.mongo.test.utils.watch :as wu]
   [spy.core :as spy])
  (:import [com.mongodb MongoChangeStreamException]
           [org.bson.types ObjectId]))

(use-fixtures :each #'dbu/test-suite-db-fixture #'dbu/test-case-db-fixture #'wu/with-callbacks)

(deftest collection-update
  (testing "One upsert that triggers an insert, one update"
    (m/create-collection! :coll)
    (w/on-update :coll wu/on-watch)
    (w/on-insert :coll wu/on-watch-2)
    (let [id (ObjectId.)]
      (m/set-one! :coll {:_id id} {:index 0} :upsert? true)
      (m/set-one! :coll {:_id id} {:index 1})
      (testing "Non-update is ignored"
        (m/set-one! :coll {:_id (ObjectId.)} {:index 1}))
      (Thread/sleep 200)
      (is (spy/called-once? wu/on-watch-2))
      (is (spy/called-once? wu/on-watch))
      (is (spy/call-matching? wu/on-watch-2
                              (fn [[_ _ delta]]
                                (= {:_id id
                                    :index 0}
                                   delta))))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ _ delta]]
                                (= {:_id id
                                    :index 1}
                                   delta)))))))

(deftest filter-update
  (testing "Filter on field"
    (let [id (:_id (m/insert! :coll {:index 0}))]
      (w/on-update :coll wu/on-watch :filter {:index {:$lt 2}})
      (m/set-one! :coll {:_id id} {:index 1})
      (m/set-one! :coll {:_id id} {:index 2})
      (Thread/sleep 200)
      (is (spy/called-once? wu/on-watch))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ _ delta]]
                                (= {:_id id
                                    :index 1}
                                   delta)))))))

(deftest collation
  (m/create-collection! :coll)
  (w/on-update :coll wu/on-watch :filter {:num "12"})
  (w/on-update :coll wu/on-watch-2 :filter {:num "12"} :collation (m/collation "se" :alternate :shifted))
  (let [id (:_id (m/insert! :coll {:num "1"}))]
    (m/set! :coll {:_id id} {:num "12"})
    (m/set! :coll {:_id id} {:num ".12 "})
    (Thread/sleep 200)
    (testing "Exact match without collation"
      (is (spy/called-once? wu/on-watch)))
    (testing "Match with collation"
      (is (spy/called-n-times? wu/on-watch-2 2)))))

(deftest filter-full-update
  (testing "Filter on field in full doc stream"
    (m/create-collection! :coll :full-change? true)
    (let [id (:_id (m/insert! :coll {:index 0}))]
      (w/on-update :coll wu/on-watch :filter {:prop {:$exists 1}} :full? true)
      (m/set-one! :coll {:_id id} {:prop "prop"})
      (m/update-one! :coll {:_id id} {:$unset {:prop 1}})
      (Thread/sleep 200)
      (is (spy/called-once? wu/on-watch))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ _ delta old-doc]]
                                (and (= {:_id id
                                         :prop "prop"}
                                        delta)
                                     (= {:_id id
                                         :index 0}
                                        old-doc))))))))

(deftest full-update
  (testing "Update stream passes full documents"
    (m/create-collection! :coll :full-change? true)
    (let [id   (:_id (m/insert! :coll {:prop "prop"
                                       :index 0}))]
      (w/on-update :coll wu/on-watch :full? true)
      (w/on-update :coll wu/on-watch-2)
      (m/set-one! :coll {:_id id} {:index 1})
      (Thread/sleep 200)
      (is (spy/called-once? wu/on-watch))
      (is (spy/called-once? wu/on-watch-2))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ _ delta old-doc]]
                                (and (= {:_id id
                                         :index 1}
                                        delta)
                                     (= {:_id id
                                         :prop "prop"
                                         :index 0}
                                        old-doc)))))
      (testing "Create delta stream still works"
        (is (spy/call-matching? wu/on-watch-2
                                (fn [[_ _ delta]]
                                  (= {:_id id
                                      :index 1}
                                     delta))))))))

(deftest stream-fail-and-modify
  (m/create-collection! :coll)
  (let [prop (ObjectId.)
        id   (:_id (m/insert! :coll {:prop prop
                                     :index 0}))]
    (testing "Create full doc change stream fails"
      (is (thrown-with-msg? MongoChangeStreamException
                            #""
                            (w/on-update :coll wu/on-watch :full? true))))
    (testing "Modify collection to accept full change stream"
      (m/modify-collection! :coll :full-change? true)
      (w/on-update :coll wu/on-watch :full? true)
      (m/set-one! :coll {:_id id} {:index 1})
      (Thread/sleep 200)
      (is (spy/called-once? wu/on-watch)))))

(deftest database-stream
  (testing "Database-wide update stream"
    (w/on-update nil wu/on-watch)
    (m/create-collection! :coll-1)
    (m/create-collection! :coll-2 :full-change? true)
    (let [id-1 (:_id (m/insert! :coll-1 {:index 0}))
          id-2 (:_id (m/insert! :coll-2 {:index 0}))]
      (m/set-one! :coll-1 {:_id id-1} {:index 1})
      (m/set-one! :coll-2 {:_id id-2} {:index 2})
      (Thread/sleep 200)
      (is (spy/called-n-times? wu/on-watch 2))
      (is (spy/call-matching? wu/on-watch
                              (fn [[coll _ delta]]
                                (and (= :coll-1 coll)
                                     (= {:_id id-1
                                         :index 1}
                                        delta)))))
      (testing "Setting full stream docs is ignored"
        (is (spy/call-matching? wu/on-watch
                                (fn [[coll _ delta]]
                                  (and (= :coll-2 coll)
                                       (= {:_id id-2
                                           :index 2}
                                          delta)))))))))

(deftest full-database-stream
  (testing "A database-wide update stream passing full documents"
    (w/on-update nil wu/on-watch :full? true)
    (m/create-collection! :coll-1 :full-change? true)
    (m/create-collection! :coll-2)
    (let [prop (ObjectId.)
          id-1 (:_id (m/insert! :coll-1 {:prop prop
                                         :index 0}))
          id-2 (:_id (m/insert! :coll-2 {:prop prop
                                         :index 0}))]
      (m/set-one! :coll-1 {:_id id-1} {:index 1})
      (m/set-one! :coll-2 {:_id id-2} {:index 2})
      (Thread/sleep 200)
      (m/set-one! :coll-1 {:_id id-1} {:index 3})
      (Thread/sleep 200)
      (is (spy/called-n-times? wu/on-watch 3))
      (is (spy/call-matching? wu/on-watch
                              (fn [[_ _ delta old-doc]]
                                (and (= {:_id id-1
                                         :index 1}
                                        delta)
                                     (= {:_id id-1
                                         :prop prop
                                         :index 0}
                                        old-doc)))))
      (testing "Not setting collection to full update makes a delta watch"
        (is (spy/call-matching? wu/on-watch
                                (fn [[_ _ delta]]
                                  (= {:_id id-2
                                      :index 2}
                                     delta)))))
      (testing "Watch still working after failure"
        (is (spy/call-matching? wu/on-watch
                                (fn [[_ _ delta old-doc]]
                                  (and (= {:_id id-1
                                           :index 3}
                                          delta)
                                       (= {:_id id-1
                                           :prop prop
                                           :index 1}
                                          old-doc)))))))))
