(ns com.timezynk.mongo.test.fetch-and-replace
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest bad-replace
  (testing "Replace with nil"
    (is (thrown-with-msg? IllegalArgumentException
                          #"replacement can not be null"
                          (m/fetch-and-replace-one! :coll
                                                    {}
                                                    nil))))
  (testing "Replace with empty list"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Invalid pipeline for an update. The pipeline may not be empty."
                          (m/fetch-and-replace-one! :coll
                                                    {}
                                                    [])))
    (is (thrown-with-msg? IllegalArgumentException
                          #"Invalid pipeline for an update. The pipeline may not be empty."
                          (m/fetch-and-replace-one! :coll
                                                    {}
                                                    '())))))

(deftest replace-and-fetch-old
  (m/insert! :coll {:name "1"})
  (is (= {:name "1"}
         (-> (m/fetch-and-replace-one! :coll
                                       {}
                                       {:mail "2"})
             (dissoc :_id))))
  (is (= {:mail "2"}
         (-> (m/fetch-one :coll)
             (dissoc :_id))))

  (deftest replace-and-fetch-new
    (m/insert! :coll {:name "1"})
    (is (= {:mail "2"}
           (-> (m/fetch-and-replace-one! :coll
                                         {}
                                         {:mail "2"}
                                         :return-new? true)
               (dissoc :_id))))))

(deftest upsert-and-fetch-old
  (is (nil? (m/fetch-and-replace-one! :coll
                                      {}
                                      {:mail "2"}
                                      :upsert? true)))
  (is (= {:mail "2"}
         (-> (m/fetch-one :coll)
             (dissoc :_id)))))

(deftest upsert-and-fetch-new
  (is (= {:mail "2"}
         (-> (m/fetch-and-replace-one! :coll
                                       {}
                                       {:mail "2"}
                                       :upsert? true
                                       :return-new? true)
             (dissoc :_id)))))
