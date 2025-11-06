(ns com.timezynk.mongo.test.set
  (:require
   [clojure.core.reducers :as r]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [com.mongodb MongoWriteException]
           [org.bson.codecs.configuration CodecConfigurationException]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest simple-set
  (let [res (m/insert! :coll {:name "A"})]
    (is (= {:matched-count 1
            :modified-count 1
            :acknowledged true}
           (m/set! :coll
                   {:_id (:_id res)}
                   {:email "test@test.com"})))
    (is (= "test@test.com"
           (:email (m/fetch-one :coll {}))))))

(deftest bad-set
  (testing "Set with nil"
    (is (thrown-with-msg? MongoWriteException
                          #"Modifiers operate on fields but we found type null instead"
                          (m/set! :coll
                                  {}
                                  nil))))
  (testing "Set with empty list"
    (is (thrown-with-msg? MongoWriteException
                          #"Modifiers operate on fields but we found type array instead"
                          (m/set! :coll
                                  {}
                                  [])))
    (is (thrown-with-msg? CodecConfigurationException
                          #"Can't find a codec for CodecCacheKey"
                          (m/set! :coll
                                  {}
                                  '()))))
  (testing "Modifiers are handled as regular fields"
    (is (= {:matched-count 0
            :modified-count 0
            :acknowledged true}
           (m/set! :coll
                   {}
                   {:$set {:a 1}})))))

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
