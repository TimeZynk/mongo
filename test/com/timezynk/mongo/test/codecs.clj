(ns com.timezynk.mongo.test.codecs
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu])
  (:import [java.security MessageDigest]
           [java.util Arrays Date UUID]
           [java.util.regex Pattern]
           [org.bson BsonDecimal128 BsonType BsonUndefined]
           [org.bson.codecs BsonDecimal128Codec BsonUndefinedCodec Codec]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest date
  (let [d (Date.)]
    (m/insert! :coll {:d d})
    (let [res (:d (m/fetch-one :coll))]
      (is (= Date (type res)))
      (is (= d res)))))

(deftest decimal
  (m/insert! :coll
             {:d 123.456M})
  (let [res (:d (m/fetch-one :coll))]
    (is (= BigDecimal (type res)))
    (is (= 123.456M res)))
  (m/with-codecs [(BsonDecimal128Codec.)]
                 {BsonType/DECIMAL128 BsonDecimal128}
    (let [res (:d (m/fetch-one :coll))]
      (is (= BsonDecimal128 (type res))))))

(deftest regex
  (let [reg #"[123]+"]
    (is (= Pattern (type reg)))
    (m/insert! :coll {:regex reg})
    (let [res (:regex (m/fetch-one :coll))]
      (is (= Pattern (type res)))
      (is (= "[123]+" (.pattern res))))))

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
      (is (= Long
             (-> (m/fetch-one :coll {:_id id-1})
                 :int
                 type)))
      (is (= Long
             (m/with-codecs codec {}
               (-> (m/fetch-one :coll {:_id id-2})
                   :int
                   type)))))))

(deftest uuid
  (let [uuid (UUID/randomUUID)]
    (m/insert! :coll {:a uuid})
    (let [res (:a (m/fetch-one :coll))]
      (is (= UUID (type res)))
      (is (= uuid res)))))

(deftest test-byte-array
  (let [a (byte-array [1 2 3])]
    (m/insert! :coll {:a a})
    ; = sign doesn't work
    (is (Arrays/equals a (:a (m/fetch-one :coll))))))

(deftest md5-hash
  (let [hash (-> (MessageDigest/getInstance "MD5")
                 (.digest (byte-array [1 2 3 4 5])))]
    (m/insert! :coll {:a hash})
    (is (Arrays/equals hash (:a (m/fetch-one :coll))))))
