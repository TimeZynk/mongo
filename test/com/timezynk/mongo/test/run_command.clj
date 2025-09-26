(ns com.timezynk.mongo.test.run-command
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.timezynk.mongo :as m]
   [com.timezynk.mongo.test.utils.db-utils :as dbu]))

(use-fixtures :once #'dbu/test-suite-db-fixture)
(use-fixtures :each #'dbu/test-case-db-fixture)

(deftest server-status
  (testing "Plain call"
    (is (= #{:asserts :connections}
           (-> (m/server-status)
               (select-keys [:asserts :connections])
               keys
               set))))
  (testing "Filter data"
    (is (= #{:$clusterTime
             :asserts
             :host
             :localTime
             :ok
             :operationTime
             :pid
             :process
             :service
             :uptime
             :uptimeEstimate
             :uptimeMillis
             :version}
           (->> (m/server-status :asserts)
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
