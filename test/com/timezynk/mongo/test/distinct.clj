(ns com.timezynk.mongo.test.distinct
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.schema :as s]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest fetch-distinct
  (m/insert! :coll [{:name "A"}
                    {:name "A"}
                    {:name "C"}
                    {:name "C"}
                    {:name "B"}])
  (is (= [] (m/distinct :coll :a)))
  (is (= ["A" "B" "C"] (m/distinct :coll :name)))
  (is (= ["A" "B"] (m/distinct :coll :name {:name {:$lte "B"}})))
  (is (thrown-with-msg? IllegalArgumentException
                        #"Not part of schema: name"
                        (m/distinct :coll :name {} :validate? true))))

(deftest fetch-in
  (m/create-collection! :coll :schema {:a (s/map {:b (s/integer)})})
  (m/insert! :coll [{:a {:b 1}}
                    {:a {:b 1}}
                    {:a {:b 2}}])
  (is (= [1 2] (m/distinct :coll :a.b {} :validate? true)))
  (is (= [{:b 1} {:b 2}] (m/distinct :coll :a {} :validate? true)))
  (is (thrown-with-msg? IllegalArgumentException
                        #"Not part of schema: a\.a"
                        (m/distinct :coll :a.a {} :validate? true))))
