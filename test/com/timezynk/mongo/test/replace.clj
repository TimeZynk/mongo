(ns com.timezynk.mongo.test.replace
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest empty-replace
  (testing "Update with empty list"
    (let [res (m/replace-one! :coll
                              {}
                              [])]
      (is (= {:matched-count 0
              :modified-count 0}
             res)))
    (let [res (m/replace-one! :coll
                              {}
                              '())]
      (is (= {:matched-count 0
              :modified-count 0}
             res)))))

(deftest simple-replace
  (testing "Create a document, replace it"
    (let [res (m/insert! :companies {:name "1"})]
      (is (= {:matched-count 1
              :modified-count 1}
             (m/replace-one! :companies
                             {:_id (:_id res)}
                             {:username "2"})))
      (is (= {:username "2"}
             (select-keys (m/fetch-one :companies {})
                          [:name :username]))))))

(deftest bad-replace
  (testing "Replace with nil"
    (is (thrown-with-msg? IllegalArgumentException
                          #"replacement can not be null"
                          (m/replace-one! :coll
                                          {}
                                          nil))))
  (testing "Using $set, as in an update, should not work"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Invalid BSON field"
                          (m/replace-one! :companies
                                          {}
                                          {:$set {:email "test@test.com"}})))))

(deftest upsert
  (testing "Upsert flag should create the document"
    (m/replace-one! :companies {} {:name "Company"})
    (is (= 0 (count (m/fetch :companies {}))))
    (m/replace-one! :companies {} {:name "Company"} :upsert? true)
    (is (= 1 (count (m/fetch :companies {}))))))
