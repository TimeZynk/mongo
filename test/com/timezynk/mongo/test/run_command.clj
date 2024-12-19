(ns com.timezynk.mongo.test.run-command
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest server-status
  (testing "Number of fields returned"
    (is (= 55
           (count (keys (m/server-status))))))
  (testing "Filter data"
    (is (= #{:service
             :localTime
             :uptimeEstimate
             :uptime
             :process
             :operationTime
             :uptimeMillis
             :host
             :pid
             :ok
             :version
             :$clusterTime
             :asserts
             :queues}
           (->> (m/server-status :asserts :queues)
                keys
                (into #{}))))))

(deftest run-command
  (is (= #{:authenticatedUserRoles
           :authenticatedUsers}
         (->> (m/run-command! :connectionStatus 1
                              :showPrivileges false)
              :authInfo
              keys
              (into #{}))))
  (is (= #{:authenticatedUserPrivileges
           :authenticatedUserRoles
           :authenticatedUsers}
         (->> (m/run-command! :connectionStatus 1
                              :showPrivileges true)
              :authInfo
              keys
              (into #{})))))