(defproject jepsen.metadb "0.1.0"
  :description "Official Jepsen (jepsen.io) fault-injection & consistency test suite for RustFS MetaDB"
  :url "https://github.com/rustfs/MetaDB"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.11.4"]
                 [jepsen "0.3.8"]
                 [elle "0.2.3"]
                 [knossos "0.3.12"]
                 [cheshire "5.13.0"]]
  :jvm-opts ["-Djava.awt.headless=true"
             "-Xmx4g"]
  :main jepsen.metadb
  :profiles {:uberjar {:aot :all}})
