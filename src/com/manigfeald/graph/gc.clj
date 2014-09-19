(ns com.manigfeald.graph.gc
  (:require [clojure.java.jdbc :as jdbc]
            [com.manigfeald.kvgc :as k]
            [com.manigfeald.graph.rel :as r])
  (:import (java.util HashMap)))

;; fascinatingly meta-circular, garbage collecting a graph, because
;; garbage collection is a graph algorithm

(defn ^:dynamic gs-references [con config]
  (let [ng (r/as (r/t (:named-graph config) :id :graph_id :name :tag) :ng)
        g (r/as (r/t (:graph config) :id :tag) :g)
        f (r/as (r/t (:fragment config) :id :tag :size) :g)
        gfs (r/as (r/t (:graph-fragments config)
                       :id :graph_id :fragment_id :tag) :gfs)
        e (r/as (r/t (:edge config)
                     :id :fragment_id :vid :src :dest :weight :tag)
                :e)
        n (r/as (r/t (:node config) :id :fragment_id :vid :tag) :n)
        h (fn [table k]
            (rest (jdbc/query con
                              (r/to-sql (apply r/π table
                                               (for [c (r/columns table)
                                                     :when (= "id" (name c))]
                                                 c)))
                              :as-arrays? true
                              :row-fn (fn [[id]] [k id]))))]
    (for [[k v] config
          i (case k
              :named-graph (h ng k)
              :graph (h g k)
              :fragment (h f k)
              :graph-fragments (h gfs k)
              :edge (h e k)
              :node (h n k)
              (if (= "attribute" (namespace k))
                (map
                 (juxt (constantly k) :id)
                 (jdbc/query con [(format "SELECT id FROM %s" v)]))
                (assert nil [k v])))]
      [i nil])))

(declare ^:dynamic tags)
(declare ^:dynamic free-queue)

(defrecord Heap [con config mark-state]
  k/Heap
  (references [heap ptr]
    (let [ng (r/as (r/t (:named-graph config) :id :graph_id :name :tag) :ng)
          g (r/as (r/t (:graph config) :id :tag) :g)
          f (r/as (r/t (:fragment config) :id :tag :size) :f)
          gfs (r/as (r/t (:graph-fragments config)
                         :id :graph_id :fragment_id :tag) :gfs)
          [table id] ptr
          _ (assert table)
          _ (assert id)
          x (case table
              ;; TODO: fragments should not reference attributes,
              ;; nodes and edges should reference their attributes
              :named-graph (rest
                            (jdbc/query
                             con (r/to-sql (r/π ng :ng/graph_id))
                             :as-arrays? true
                             :row-fn (fn [[id]] [:graph id])))
              :graph (rest (jdbc/query
                            con (r/to-sql
                                 (->
                                  (r/σ gfs (r/≡ :gfs/graph_id (r/lit id)))
                                  (r/π :gfs/id)) )
                            :as-arrays? true
                            :row-fn (fn [[id]] [:graph-fragments id])))
              :fragment (doall (for [[k v] config
                                     :when (not
                                            (contains?
                                             #{:named-graph :graph
                                               :fragment :graph-fragments}
                                             k))
                                     item (rest
                                           (jdbc/query
                                            con
                                            (-> (r/as (r/t (k config) :fragment_id :id) :t)
                                                (r/σ (r/≡ :t/fragment_id (r/lit id)))
                                                (r/π :t/id)
                                                (r/to-sql))
                                            :as-arrays? true
                                            :row-fn (fn [[id]] [k id])))]
                                 item))
              :graph-fragments (->> (jdbc/query
                                     con
                                     (r/to-sql (r/π gfs
                                                    :gfs/graph_id
                                                    :gfs/fragment_id))
                                     :as-arrays? true
                                     :row-fn (fn [[graph-id fragment-id]]
                                               [:fragment fragment-id]))
                                    (rest))
              [])]
      (assert (not (seq (for [[k v] x
                              :when (or (nil? k) (nil? v))]
                          k)))
              [table (first (for [[k v] x
                                  :when (or (nil? k) (nil? v))]
                              k))])
      (assert (not (seq (for [[k v] x
                              :when (not (number? v))]
                          true)))
              (first (for [[k v] x
                           :when (not (number? v))]
                       [table [k v]])))
      x))
  (tag [heap ptr tag-value]
    (.put ^HashMap tags ptr tag-value)
    heap)
  (tag-value [heap ptr]
    (.get ^HashMap tags ptr))
  (free [heap ptr]
    (let [[table id] ptr
          _ (assert id table)
          fq (update-in free-queue [table] (fnil conj #{}) id)
          fq (reduce
              (fn [fq [table ids]]
                (if (and (seq ids)
                         (or (> (count ids) 10)
                             (#{:fragment :graph-fragments :graph :named-graph} table)))
                  (let [table-name (get config table)
                        [a b] (reduce
                               (fn [[a b] [c d]]
                                 [(conj a c)
                                  (conj b d )])
                               [[] []]
                               (for [id ids] ["id = ?" id]))
                        q (into [(apply str (interpose " OR " a))] b)]
                    (jdbc/delete! con table-name q)
                    (update-in fq [table] empty))
                  fq))
              fq
              fq)]
      (set! free-queue fq)
      heap))
  (with-heap-lock [heap fun]
    (binding [tags (HashMap. 32)
              free-queue {}]
      (fun heap)))
  clojure.core.protocols/CollReduce
  (coll-reduce [_ fun]
    (reduce fun (gs-references con config)))
  (coll-reduce [_ fun init]
    (reduce fun init (gs-references con config))))
