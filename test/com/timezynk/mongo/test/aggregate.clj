(ns com.timezynk.mongo.test.aggregate
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
  ;;  [clojure.tools.logging :as log]
   [com.timezynk.mongo :as mongo]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest simple-aggregate
  (mongo/insert! :companies [{:name "1" :duplicity 1}
                             {:name "2" :duplicity 2}
                             {:name "3" :duplicity 3}])
  (is (= #{{:duplicity 2} {:duplicity 3}}
         (->> (mongo/aggregate :companies
                               {:$match {:duplicity {:$gte 2}}}
                               {:$project {:_id 0
                                           :duplicity 1}})
              (into #{})))))
