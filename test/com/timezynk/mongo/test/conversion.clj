(ns com.timezynk.mongo.test.conversion
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [org.joda.time DateTime LocalDate LocalDateTime LocalTime]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest test-keyword
  (testing "Keyword is converted to string"
    (m/insert! :coll
               {:keyword :keyword})
    (is (= "keyword" (:keyword (m/fetch-one :coll))))))

(deftest test-namespace
  (testing "Keyword with a slash preserves the slash"
    (m/insert! :coll
               {:my/keyword :your/keyword})
    (is (= "your/keyword" (:my/keyword (m/fetch-one :coll))))))

(deftest test-localdate
  (testing "org.joda.time.LocalDate is converted to String"
    (m/insert! :coll
               {:local-date (LocalDate.)})
    (is (= String (-> (m/fetch-one :coll)
                      :local-date
                      type)))))

(deftest test-localdatetime
  (testing "org.joda.time.LocalDateTime is converted to String"
    (m/insert! :coll
               {:local-datetime (LocalDateTime.)})
    (is (= String (-> (m/fetch-one :coll)
                      :local-datetime
                      type)))))

(deftest test-localtime
  (testing "org.joda.time.LocalTime is converted to String"
    (m/insert! :coll
               {:local-time (LocalTime.)})
    (is (= String (-> (m/fetch-one :coll)
                      :local-time
                      type)))))

(deftest test-datetime
  (testing "org.joda.time.DateTime is converted to String"
    (m/insert! :coll
               {:datetime (DateTime.)})
    (is (= String (-> (m/fetch-one :coll)
                      :datetime
                      type)))))

(deftest test-set
  (testing "A clojure set is converted to vec"
    (m/insert! :coll
               {:set #{{:a #{:b}}}})
    (let [res (:set (m/fetch-one :coll))]
      (is (= [{:a ["b"]}] res)))))
