(ns com.timezynk.mongo.test.replace
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
  ;;  [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest simple-replace
  (let [res (mongo/insert! :companies {:name "1"})]
    (is (= {:matched-count 1
            :modified-count 1}
           (mongo/replace-one! :companies
                           {:_id (:_id res)}
                           {:username "2"})))
    (is (= {:username "2"}
           (select-keys (mongo/fetch-one :companies {})
                        [:name :username])))))

(deftest bad-replace
  (is (thrown-with-msg? Exception #"Invalid BSON field"
                        (mongo/replace-one! :companies
                                        {}
                                        {:$set {:email "test@test.com"}}))))

(deftest upsert
  (mongo/replace-one! :companies {} {:name "Company"})
  (is (= 0 (count (mongo/fetch :companies {}))))
  (mongo/replace-one! :companies {} {:name "Company"} :upsert? true)
  (is (= 1 (count (mongo/fetch :companies {})))))
