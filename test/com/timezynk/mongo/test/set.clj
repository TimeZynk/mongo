(ns com.timezynk.mongo.test.set
  (:require
   [clojure.core.reducers :as r]
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest simple-set
  (let [res (m/insert! :coll {:name "A"})]
    (is (= {:matched-count 1
            :modified-count 1
            :_id nil
            :acknowledged true}
           (m/set! :coll
                   {:_id (:_id res)}
                   {:email "test@test.com"})))
    (is (= "test@test.com"
           (:email (m/fetch-one :coll {}))))))

(deftest set-one
  (m/insert! :coll {:name "A"})
  (m/insert! :coll {:name "A"})
  (m/set-one! :coll
              {}
              {:name "B"})
  (is (= #{"A" "B"}
         (->> (m/fetch :coll {})
              (r/map :name)
              (into #{})))))

(deftest fetch-and-set-one
  (m/insert! :coll {:name "A"})
  (is (= "A" (:name (m/fetch-and-set-one! :coll {:name "A"} {:name "B"}))))
  (is (= "C" (:name (m/fetch-and-set-one! :coll {:name "B"} {:name "C"} :return-new? true)))))
