(ns com.timezynk.mongo.test.distinct
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest fetch-distinct
  (m/create-collection! :coll :schema {:name (s/string)})
  (m/insert! :coll [{:name "A"}
                    {:name "A"}
                    {:name "C"}
                    {:name "C"}
                    {:name "B"}])
  (is (= [] (m/fetch-distinct :coll :a)))
  (is (= ["A" "B" "C"] (m/fetch-distinct :coll :name)))
  (is (= ["A" "B"] (m/fetch-distinct :coll :name {:name {:$lte "B"}}))))

(deftest fetch-in
  (m/create-collection! :coll :schema {:a (s/map {:b (s/integer)})})
  (m/insert! :coll [{:a {:b 1}}
                    {:a {:b 2}}])
  (is (= [1 2] (m/fetch-distinct :coll :a.b {} :validate? true)))
  (is (thrown-with-msg? IllegalArgumentException
                        #"Not part of schema: a\.a"
                        (m/fetch-distinct :coll :a.a {} :validate? true))))
