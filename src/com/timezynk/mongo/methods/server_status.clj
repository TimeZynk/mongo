(ns ^:no-doc com.timezynk.mongo.methods.server-status
  (:require
   [com.timezynk.mongo.codecs.bson :refer [->bson]]
   [com.timezynk.mongo.config :refer [*mongo-database* *mongo-session*]])
  (:import [clojure.lang PersistentArrayMap]
           [java.util LinkedHashMap]))

(defonce parts #{:asserts
                 :batchedDeletes
                 :bucketCatalog
                 :catalogStats
                 :changeStreamPreImages
                 :collectionCatalog
                 :connections
                 :defaultRWConcern
                 :electionMetrics
                 :extra_info
                 :featureCompatibilityVersion
                 :flowControl
                 :globalLock
                 :hedgingMetrics
                 :indexBuilds
                 :indexBulkBuilder
                 :indexStats
                 :internalTransactions
                 :locks
                 :logicalSessionRecordCache
                 :mem
                 :metrics
                 :mirroredReads
                 :network
                 :opLatencies
                 :opWorkingTime
                 :opReadConcernCounters
                 :opWriteConcernCounters
                 :opcounters
                 :opcountersRepl
                 :oplogTruncation
                 :planCache
                 :queryAnalyzers
                 :querySettings
                 :queryStats
                 :queues
                 :readConcernCounters
                 :readPreferenceCounters
                 :repl
                 :scramCache
                 :security
                 :shardedIndexConsistency
                 :shardingStatistics
                 :shardSplits
                 :storageEngine
                 :tcmalloc
                 :tenantMigrations
                 :trafficRecording
                 :transactions
                 :transportSecurity
                 :twoPhaseCommitCoordinator
                 :watchdog
                 :wiredTiger})

(def ^:private parts-map
  (zipmap parts (repeatedly (fn [] 0))))

(defn- build-params [options]
  (let [map (LinkedHashMap.)]
    (.put map :serverStatus 1)
    (when options
      (.putAll map (merge parts-map
                          (zipmap options (repeatedly (fn [] 1))))))
    (->bson map)))

(defmulti server-status-method
  (fn [_options] (some? *mongo-session*)))

(defmethod server-status-method true [options]
  (.runCommand *mongo-database*
               *mongo-session*
               (build-params options)
               PersistentArrayMap))

(defmethod server-status-method false [options]
  (.runCommand *mongo-database*
               (build-params options)
               PersistentArrayMap))
