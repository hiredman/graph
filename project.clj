(defproject com.manigfeald/graph "0.2.0"
  :description "FIXME: write description"
  :url "http://github.com/hiredman/graph"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.derby/derby "10.10.2.0"]
                 [org.clojure/java.jdbc "0.3.5"]
                 [aysylu/loom "0.5.0"]
                 [com.manigfeald/kvgc "0.2.0"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.5.9"]]}})
