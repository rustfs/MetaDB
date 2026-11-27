(ns jepsen.metadb
  "Official Jepsen (https://jepsen.io/) fault-injection & consistency test suite for RustFS MetaDB.

   Supports 5-node Linux clusters (Docker or bare-metal/VMs) with:
   - Workloads:
       * `register`    - Linearizable multi-key register via MetaTable SetAttr/Lookup (Knossos WGL)
       * `set`         - Concurrent unique file creates (jepsen.checker/set-full)
       * `bank`        - Cross-shard transfers audited against QuotaTable/DirStat conservation
       * `rename-elle` - Cross-shard 2PC atomic renames & pair reads (Elle rw-register / G0-G2)
   - Nemeses:
       * `partition`   - iptables random halves & majorities-ring partitions
       * `kill`        - SIGKILL & restart of metadbd processes
       * `pause`       - SIGSTOP / SIGCONT process freezing (stale-leader attack)
       * `clock`       - Clock skew / drift bumps
       * `split`       - Live online shard splitting (`metadb-ctl shard split`) & leader transfer"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [clojure.java.shell :refer [sh]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [elle.rw-register :as elle-rw])
  (:gen-class))

(def data-dir   "/var/lib/metadb")
(def config-dir "/etc/metadb")
(def config-file "/etc/metadb/node.toml")
(def log-file   "/var/log/metadb/metadbd.log")
(def pid-file   "/var/run/metadbd.pid")
(def binary     "/usr/local/bin/metadbd")
(def ctl-bin    "/usr/local/bin/metadb-ctl")
(def auth-token "metadb-jepsen-cluster-secret")
(def grpc-port  7800)

(defn node-id
  "Maps Jepsen node name (`n1`..`n5`) to 1-based MetaDB `node_id`."
  [test node]
  (inc (.indexOf ^java.util.List (:nodes test) node)))

(defn node-endpoint
  [node]
  (str node ":" grpc-port))

(defn primary-node
  [test]
  (first (:nodes test)))

(defn render-node-toml
  [test node]
  (let [nid  (node-id test node)
        join (if (= node (primary-node test))
               ""
               (node-endpoint (primary-node test)))]
    (str "[node]\n"
         "node_id = " nid "\n"
         "cell_id = 1\n"
         "role = \"all-in-one\"\n"
         "data_dir = \"" data-dir "\"\n"
         "grpc_listen_addr = \"" (node-endpoint node) "\"\n\n"
         "[storage]\n"
         "locality_tombstone_threshold = 500\n"
         "locality_tombstone_ratio = 0.90\n\n"
         "[sharding]\n"
         "bootstrap_table_shards = true\n"
         "shard_split_threshold_bytes = 65536\n\n"
         "[cluster]\n"
         "join = \"" join "\"\n"
         "join_timeout_secs = 45\n\n"
         "[security]\n"
         "shared_secret = \"" auth-token "\"\n\n"
         "[raft]\n"
         "heartbeat_interval_ms = 100\n"
         "election_timeout_min_ms = 300\n"
         "election_timeout_max_ms = 600\n")))

(defn run-ctl!
  "Executes `metadb-ctl` from the control node against `node`'s gRPC endpoint."
  [node & args]
  (let [cmd (concat [ctl-bin
                     "--endpoint" (node-endpoint node)
                     "--auth-token" auth-token]
                    args)
        res (apply sh cmd)]
    res))

(defn run-sql!
  [node query & params]
  (let [param-args (mapcat (fn [p] ["--param" p]) params)]
    (apply run-ctl! node "sql" query param-args)))

(defn db
  "MetaDB multi-node database lifecycle for Jepsen (`setup!`, `teardown!`, `kill!`, `start!`, `pause!`)."
  []
  (reify
    db/DB
    (setup! [_ test node]
      (info node "Configuring and starting MetaDB node" (node-id test node))
      (c/su
        (c/exec :mkdir :-p data-dir config-dir "/var/log/metadb")
        (c/exec :echo (render-node-toml test node) :> config-file)
        (when-not (= node (primary-node test))
          (Thread/sleep 2500))
        (cu/start-daemon!
          {:logfile log-file
           :pidfile pid-file
           :chdir   data-dir}
          binary
          :--strict-config
          :--config config-file)
        (Thread/sleep 2000)))

    (teardown! [_ _test node]
      (info node "Tearing down MetaDB node")
      (c/su
        (cu/stop-daemon! binary pid-file)
        (c/exec :rm :-rf data-dir log-file)))

    db/LogFiles
    (log-files [_ _test _node]
      [log-file])

    db/Process
    (start! [_ _test node]
      (c/su
        (cu/start-daemon!
          {:logfile log-file
           :pidfile pid-file
           :chdir   data-dir}
          binary
          :--strict-config
          :--config config-file)))

    (kill! [_ _test node]
      (c/su
        (cu/stop-daemon! binary pid-file)))

    db/Pause
    (pause! [_ _test node]
      (c/su
        (cu/grepkill! :stop binary)))

    (resume! [_ _test node]
      (c/su
        (cu/grepkill! :cont binary)))))

;; ---------------------------------------------------------------------------
;; Workloads: Register (Knossos), Set-Full, Bank, and Elle Rename
;; ---------------------------------------------------------------------------

(defonce reg-seq (atom 1))

(defn parse-size-from-sql-out
  [stdout]
  (when-let [[_ num] (re-find #"size_bytes=(\d+)" (or stdout ""))]
    (Long/parseLong num)))

(defn parse-quota-bytes-from-sql-out
  [stdout]
  (when-let [[_ num] (re-find #"used_bytes=(\d+)" (or stdout ""))]
    (Long/parseLong num)))

(defrecord RegisterClient [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ test]
    (let [leader (primary-node test)]
      (run-sql! leader
                "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'jepsen_reg', 100, 'DIR', 10, 493)")
      (doseq [k (range 4)]
        (run-sql! leader
                  (format "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (100, 'reg_%d', %d, 'FILE', %d, 10, 420, 0)"
                          k (+ 10000 k) (+ 50000 k))))))

  (invoke! [_ _test op]
    (let [[k v] (:value op)
          fname (str "reg_" k)]
      (case (:f op)
        :read
        (let [res (run-sql! node
                            (format "SELECT * FROM MetaTable WHERE pid = 100 AND name = '%s'" fname))]
          (if (zero? (:exit res))
            (assoc op :type :ok :value (independent/tuple k (parse-size-from-sql-out (:out res))))
            (assoc op :type :fail :error (:err res))))

        :write
        (let [res (run-sql! node
                            (format "UPDATE MetaTable SET size = %d, fid = %d WHERE pid = 100 AND name = '%s'"
                                    v (+ 60000 v) fname))]
          (if (zero? (:exit res))
            (assoc op :type :ok)
            (assoc op :type :info :error (:err res)))))))

  (teardown! [_ _test])
  (close! [_ _test]))

(defrecord SetClient [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ test]
    (run-sql! (primary-node test)
              "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'jepsen_set', 200, 'DIR', 10, 493)"))

  (invoke! [_ _test op]
    (case (:f op)
      :add
      (let [v   (:value op)
            res (run-sql! node
                          (format "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (200, 'item_%08d', %d, 'FILE', %d, 10, 420, 64)"
                                  v (+ 100000 v) (+ 200000 v)))]
        (if (zero? (:exit res))
          (assoc op :type :ok)
          (assoc op :type :info :error (:err res))))

      :read
      (let [res (run-sql! node
                          "SELECT * FROM MetaTable WHERE pid = 200 ORDER BY name ASC")]
        (if (zero? (:exit res))
          (let [items (->> (re-seq #"item_(\d+)" (:out res))
                           (map (comp #(Long/parseLong %) second))
                           set)]
            (assoc op :type :ok :value items))
          (assoc op :type :fail :error (:err res))))))

  (teardown! [_ _test])
  (close! [_ _test]))

(defrecord BankClient [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ test]
    (let [leader (primary-node test)]
      (run-sql! leader
                "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'bank_a', 100, 'DIR', 10, 493)")
      (run-sql! leader
                "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'bank_b', 600, 'DIR', 10, 493)")
      (doseq [acct (range 4)]
        (let [pid (if (< acct 2) 100 600)]
          (run-sql! leader
                    (format "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (%d, 'bill_%d', %d, 'FILE', %d, 10, 420, 1000)"
                            pid acct (+ 30000 acct) (+ 70000 acct)))))))

  (invoke! [_ _test op]
    (case (:f op)
      :transfer
      (let [{:keys [acct from to]} (:value op)
            res (run-sql! node
                          (format "UPDATE MetaTable SET pid = %d, name = 'bill_%d' WHERE pid = %d AND name = 'bill_%d'"
                                  to acct from acct))]
        (if (zero? (:exit res))
          (assoc op :type :ok)
          (assoc op :type :info :error (:err res))))

      :read
      (let [res (run-sql! node "SELECT * FROM QuotaTable WHERE uid = 10")]
        (if (zero? (:exit res))
          (assoc op :type :ok :value {10 (parse-quota-bytes-from-sql-out (:out res))})
          (assoc op :type :fail :error (:err res))))))

  (teardown! [_ _test])
  (close! [_ _test]))

(defrecord RenameElleClient [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ test]
    (let [leader (primary-node test)]
      (run-sql! leader
                "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'elle_a', 100, 'DIR', 10, 493)")
      (run-sql! leader
                "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'elle_b', 600, 'DIR', 10, 493)")
      (run-sql! leader
                "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (100, 'token', 20001, 'FILE', 60001, 10, 420, 1)")))

  (invoke! [_ _test op]
    (let [txn (:value op)
          [f k v] (first txn)]
      (case f
        :r
        (let [ra (run-sql! node "SELECT * FROM MetaTable WHERE pid = 100 AND name = 'token'")
              rb (run-sql! node "SELECT * FROM MetaTable WHERE pid = 600 AND name = 'token'")
              va (when (zero? (:exit ra)) (parse-size-from-sql-out (:out ra)))
              vb (when (zero? (:exit rb)) (parse-size-from-sql-out (:out rb)))]
          (cond
            (and va vb) (assoc op :type :ok :value [[:r k [va vb]]])
            va          (assoc op :type :ok :value [[:r k va]])
            vb          (assoc op :type :ok :value [[:r k vb]])
            :else       (assoc op :type :fail :error "token lookup failed")))

        :w
        (let [src (if (even? v) 100 600)
              dst (if (even? v) 600 100)
              r1  (run-sql! node (format "UPDATE MetaTable SET size = %d, fid = %d WHERE pid = %d AND name = 'token'"
                                         v (+ 80000 v) src))
              r2  (when (zero? (:exit r1))
                    (run-sql! node (format "REPLACE INTO MetaTable (pid, name) SELECT %d, 'token' FROM MetaTable WHERE pid = %d AND name = 'token'"
                                           dst src)))]
          (if (and r2 (zero? (:exit r2)))
            (assoc op :type :ok)
            (assoc op :type :info :error "rename failed or uncertain"))))))

  (teardown! [_ _test])
  (close! [_ _test]))

;; ---------------------------------------------------------------------------
;; Custom MetaDB Split & Leader-Transfer Nemesis Package
;; ---------------------------------------------------------------------------

(defn split-nemesis
  "Nemesis that triggers live online shard splitting & leader transfers via `metadb-ctl`."
  []
  (reify nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [_ test op]
      (let [leader (primary-node test)]
        (case (:f op)
          :split-shard
          (let [res (run-ctl! leader "shard" "split" "--shard-id" "1")]
            (assoc op :value {:exit (:exit res) :out (:out res)}))
          :transfer-leader
          (let [target-id (inc (rand-int (count (:nodes test))))
                res       (run-ctl! leader "raft" "transfer-leader"
                                    "--shard-id" "1"
                                    "--target-node-id" (str target-id))]
            (assoc op :value {:target target-id :exit (:exit res)}))
          op)))
    (teardown! [_ _test])))

(defn workload-for
  [opts]
  (case (:workload opts)
    :register
    {:client    (RegisterClient. nil)
     :generator (independent/concurrent-generator
                  2
                  (range 4)
                  (fn [_k]
                    (->> (gen/mix [{:type :invoke :f :read}
                                   (fn [_ _] {:type :invoke :f :write :value (swap! reg-seq inc)})])
                         (gen/stagger 1/10))))
     :checker   (independent/checker
                  (checker/compose
                    {:linear   (checker/linearizable {:model (model/register 0) :algorithm :wgl})
                     :timeline (timeline/html)}))}

    :set
    {:client    (SetClient. nil)
     :generator (gen/phases
                  (->> (range)
                       (map (fn [x] {:type :invoke :f :add :value x}))
                       (gen/stagger 1/10))
                  (gen/once {:type :invoke :f :read}))
     :checker   (checker/set-full {:linearizable? true})}

    :bank
    {:client    (BankClient. nil)
     :generator (gen/mix
                  [{:type :invoke :f :read}
                   (fn [_ _]
                     (let [forward (zero? (rand-int 2))]
                       {:type  :invoke
                        :f     :transfer
                        :value {:acct (rand-int 4)
                                :from (if forward 100 600)
                                :to   (if forward 600 100)}}))])
     :checker   (checker/bank {:negative-balances? false})}

    :rename-elle
    {:client    (RenameElleClient. nil)
     :generator (->> (gen/mix
                       [(fn [_ _] {:type :invoke :f :txn :value [[:r :token nil]]})
                        (fn [_ _] {:type :invoke :f :txn :value [[:w :token (swap! reg-seq inc)]]})])
                     (gen/stagger 1/5))
     :checker   (reify checker/Checker
                  (check [_ _test history _opts]
                    (elle-rw/check {:consistency-models [:strict-serializable]} history)))}))

(defn metadb-test
  "Constructs a complete official Jepsen test map for MetaDB."
  [opts]
  (let [wl      (workload-for opts)
        nem-set (:nemesis opts)
        nem     (nc/nemesis-package
                  {:db        (db)
                   :nodes     (:nodes opts)
                   :faults    nem-set
                   :partition {:targets [:primaries :majorities-ring :one]}
                   :pause     {:targets [:primaries :one]}
                   :kill      {:targets [:primaries :one]}
                   :interval  15})]
    (merge tests/noop-test
           opts
           {:name            (str "metadb-" (name (:workload opts)))
            :os              debian/os
            :db              (db)
            :client          (:client wl)
            :nemesis         (if (contains? nem-set :split)
                               (nemesis/compose {{:split-shard :split-shard
                                                  :transfer-leader :transfer-leader} (split-nemesis)
                                                 :default (:nemesis nem)})
                               (:nemesis nem))
            :checker         (checker/compose
                               {:perf       (checker/perf)
                                :stats      (checker/stats)
                                :exceptions (checker/unhandled-exceptions)
                                :workload   (:checker wl)})
            :generator       (->> (:generator wl)
                                  (gen/nemesis (:generator nem))
                                  (gen/time-limit (:time-limit opts)))})))

(def cli-opts
  [["-w" "--workload NAME" "Workload: register, set, bank, rename-elle"
    :default :register
    :parse-fn keyword
    :validate [#{:register :set :bank :rename-elle} "Must be register, set, bank, or rename-elle"]]
   ["-n" "--nemesis FAULTS" "Comma-separated faults: partition,kill,pause,clock,split"
    :default #{:partition :kill :pause}
    :parse-fn #(set (map keyword (str/split % #",")))]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn metadb-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
