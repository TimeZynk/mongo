(ns com.timezynk.mongo.test.hooks
  (:require
   [clojure.set :refer [rename-keys]]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.hooks :as mh]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest hooks
  (mh/with-hooks {:write (fn [doc] (rename-keys doc {:key-1 :key-2}))
                  :read  (fn [doc] (rename-keys doc {:key-2 :key-3}))}
    (let [res-1 (m/insert! :coll
                           {:key-1 "key-1"
                            :key   {:key-1 "key-key-1"}})
          res-2 (mh/ignore-hooks
                 (m/insert! :coll
                            {:key-1 "key-1"
                             :key   {:key-1 "key-key-1"}}))
          res-3 (m/fetch-one :coll
                             {:_id (:_id res-1)})
          res-4 (mh/ignore-hooks
                 (m/fetch-one :coll
                              {:_id (:_id res-1)}))]
      (testing "Insert response only adds _id"
        (is (= {:key   {:key-1 "key-key-1"}
                :key-1 "key-1"}
               (dissoc res-1 :_id))))
      (testing "Ignore hooks"
        (is (= {:key   {:key-1 "key-key-1"}
                :key-1 "key-1"}
               (dissoc res-2 :_id))))
      (testing "Read convert when fetching"
        (is (= {:key   {:key-3 "key-key-1"}
                :key-3 "key-1"}
               (dissoc res-3 :_id))))
      (testing "Fetch write converted doc without read convert"
        (is (= {:key   {:key-2 "key-key-1"}
                :key-2 "key-1"}
               (dissoc res-4 :_id)))))))
