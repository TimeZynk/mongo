(ns com.timezynk.mongo.test.conversion
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [org.bson BsonDecimal128 BsonType BsonUndefined]
           [org.bson.codecs BsonDecimal128Codec BsonUndefinedCodec Codec]
           [org.bson.types ObjectId]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest has-id
  (testing "InsertOne returns with id"
    (let [res (m/insert! :coll {:a 1})]
      (is (= ObjectId (-> res :_id type)))
      (is (= (m/fetch-one :coll) res)))))

(deftest have-ids
  (testing "InsertMany returns with ids"
    (let [res (m/insert! :coll [{:a 1}])]
      (is (= ObjectId (-> res first :_id type)))
      (is (= (m/fetch :coll) res)))))

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

(deftest test-set
  (testing "A clojure set is converted to vec"
    (m/insert! :coll
               {:set #{{:a #{:b}}}})
    (let [res (:set (m/fetch-one :coll))]
      (is (= [{:a ["b"]}] res)))))

(deftest decimal
  (m/insert! :coll
             {:d 123.456M})
  (let [res (:d (m/fetch-one :coll))]
    (is (= BigDecimal (type res)))
    (is (= 123.456M res)))
  (m/with-codecs [(BsonDecimal128Codec.)] {BsonType/DECIMAL128 BsonDecimal128}
    (let [res (:d (m/fetch-one :coll))]
      (is (= BsonDecimal128 (type res))))))

(deftest undefined
  (testing "Handling undefined values"
    (let [codec [(BsonUndefinedCodec.)]
          id-1  (:_id (m/with-codecs codec {}
                        (m/insert! :coll {:undefined (BsonUndefined.)})))
          id-2  (:_id (m/insert! :coll {:undefined (BsonUndefined.)}))]
      (is (= BsonUndefined
             (m/with-codecs codec {}
               (-> (m/fetch-one :coll {:_id id-1})
                   :undefined
                   type))))
      (is (nil? (m/with-codecs codec {}
                  (:undefined (m/fetch-one :coll {:_id id-2})))))
      (is (nil? (:undefined (m/fetch-one :coll {:_id id-1})))))))

(defn- int-codec []
  (reify Codec
    (decode [_this reader _decoder-context]
      (.readInt32 reader))

    (encode [_this writer value _encoder-context]
      (.writeInt32 writer value))

    (getEncoderClass [_this]
      Integer)))

(deftest integer
  (testing "Replace integer conversion"
    (let [codec [(int-codec)]
          id-1 (:_id (m/with-codecs codec {}
                       (m/insert! :coll {:int (int 1)})))
          id-2 (:_id (m/insert! :coll {:int (int 2)}))]
      (is (= Integer
             (m/with-codecs codec {}
               (-> (m/fetch-one :coll {:_id id-1})
                   :int
                   type))))
      (is (= Long (m/with-codecs codec {}
                    (-> (m/fetch-one :coll {:_id id-2})
                        :int
                        type))))
      (is (= Long (-> (m/fetch-one :coll {:_id id-1})
                      :int
                      type))))))
