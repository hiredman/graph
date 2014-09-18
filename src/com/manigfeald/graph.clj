(ns com.manigfeald.graph
  (:require [loom.graph :as g]
            [loom.attr :as a]
            [clojure.java.jdbc :as jdbc]
            [com.manigfeald.graph.readonly :as ro]
            [com.manigfeald.kvgc :as k]
            [com.manigfeald.graph.gc :as gc]
            [com.manigfeald.graph.alloc :refer [alloc
                                                copy-without]]
            [com.manigfeald.graph.rel :as r]
            [com.manigfeald.graph.transpose :as t]
            [com.manigfeald.graph.ddl :as ddl])
  (:import (java.util.concurrent.locks ReentrantReadWriteLock
                                       ReentrantLock
                                       Lock)
           (java.nio ByteBuffer)
           (java.lang.ref WeakReference)
           (java.util UUID)))

(defrecord GraphStore [con config rw gc-running? views references heap locks])

(defn gs [con config]
  (->GraphStore con
                config
                (ReentrantReadWriteLock.)
                (atom false)
                (atom #{})
                (atom {})
                (gc/->Heap con config (atom false))
                (atom {})))

(defn graph-store [con config]
  (gs con config))

(defn create-tables! [gs]
  ;; TODO: add indices
  (let [x (for[[k v] (:config gs)]
            (case k
              :named-graph (ddl/create-named-graph v)
              :graph (ddl/create-graph v)
              :fragment (ddl/create-fragment v)
              :graph-fragments (ddl/create-graph-fragments v)
              :edge (ddl/create-edge v)
              :node (ddl/create-node v)
              (if (= "attribute" (namespace k))
                (ddl/create-attribute v k)
                (assert nil [k v]))))]
    (apply jdbc/db-do-commands (:con gs) x)))

(defn gc [gs]
  (try
    (.lock (.writeLock ^ReentrantReadWriteLock (:rw gs)))
    (doseq [[graph-id wr] @(:references gs)
            :when (not (.get ^WeakReference wr))]
      (swap! (:views gs) disj graph-id)
      (swap! (:references gs) dissoc graph-id))
    (let [r (set
             (rest
              (jdbc/query
               (:con gs)
               (-> (r/as (r/t (:named-graph (:config gs)) :id) :ng)
                   (r/π :ng/id)
                   (r/to-sql))
               :as-arrays? true
               :row-fn (fn [[id]] [:named-graph id]))))
          [_ nms] (k/gc (:heap gs)
                        (into r (for [i @(:views gs)]
                                  [:graph i]))
                        @(:mark-state (:heap gs)))]
      (reset! (:mark-state (:heap gs)) nms))
    nil
    (finally
      (.unlock (.writeLock ^ReentrantReadWriteLock (:rw gs))))))

(defmacro with-read-lock
  "execute body with graphstore's read lock"
  [gs & body]
  `(let [gs# ~gs]
     (try
       (.lock (.readLock ^ReentrantReadWriteLock (:rw gs#)))
       ~@body
       (finally
         (.unlock (.readLock ^ReentrantReadWriteLock (:rw gs#)))))))

(declare id-graph)

(defn read-only-view [gs graph-name]
  (with-read-lock gs
    (loop [i 0]
      (when (= i 100)
        (assert nil))
      (let [g (or (first
                   (jdbc/query
                    (:con gs)
                    (-> (r/as (r/t (:named-graph (:config gs))
                                   :name :graph_id :id)
                              :ng)
                        (r/σ (r/≡ :ng/name (r/lit graph-name)))
                        (r/π :ng/name :ng/graph_id :ng/id)
                        (r/to-sql))))
                  {:graph_id (alloc gs -1)})
            ro (ro/->ROG (id-graph gs (biginteger (:graph_id g)))
                         (fn [_] (swap! (:views gs) disj (:graph_id g))))]
        (swap! (:references gs)
               (fn [m k v] (if (contains? m k) m (assoc m k v)))
               (:graph_id g) (WeakReference. ro))
        (swap! (:views gs) conj (:graph_id g))
        ro))))

(defmacro with-java-locking [lock & body]
  `(let [^java.util.concurrent.locks.Lock lock# ~lock]
     (try
       (.lock lock#)
       ~@body
       (finally
         (.unlock lock#)))))

(defn attempt-committing-new-graph [gs g ng graph-name]
  (let [ng' (r/as (r/t (:named-graph (:config gs))
                       :graph_id :id :name)
                  :ng)
        cg (or (first
                (jdbc/query
                 (:con gs)
                 (-> ng'
                     (r/σ (r/≡ :ng/id (r/lit (:id g))))
                     (r/π :ng/id :ng/graph_id)
                     (r/to-sql))))
               {:graph_id (:graph_id g)})]
    (if (= (:graph_id cg) (:graph_id g))
      (do
        (if (:new? g)
          (do
            (assert (:id ng))
            (jdbc/insert! (:con gs) (:named-graph (:config gs))
                          {:graph_id (:id ng)
                           :name graph-name}))
          (do
            (assert (:id g))
            (assert (:id ng))
            (jdbc/update! (:con gs)
                          (:named-graph (:config gs))
                          {:graph_id (:id ng)}
                          ["name = ?" graph-name])))
        true)
      false)))

(defn assoc-missing [m & pairs]
  (assert (even? (count pairs)))
  (reduce
   (fn [m [k v]]
     (if (contains? m k)
       m
       (assoc m k v)))
   m
   (partition-all 2 pairs)))

(defn graph-record [gs graph-name ng]
  (first
   (jdbc/query
    (:con gs)
    (-> ng
        (r/σ (r/≡ :ng/name (r/lit graph-name)))
        (r/π :ng/id :ng/graph_id)
        (r/to-sql)))))

(defn transact [gs graph-name fun]
  (try
    (with-read-lock gs
      (loop [i 0]
        (assert (> 100 i))
        (let [ng' (r/as (r/t (:named-graph (:config gs)) :graph_id :id :name)
                        :ng)
              g (or (graph-record gs graph-name ng')
                    {:graph_id (alloc gs -1)
                     :new? true})
              [ret ng] (fun (id-graph gs (biginteger (:graph_id g))))]
          (swap! (:locks gs) assoc-missing graph-name (ReentrantLock.))
          (assert ng)
          (assert (:id ng) (pr-str ng))
          (assert (satisfies? g/Graph ng))
          ;; TODO: don't need the lock at all, just cas using
          ;; update and check for cas success
          (if-not (with-java-locking (get (deref (:locks gs)) graph-name)
                    (attempt-committing-new-graph gs g ng graph-name))
            (recur (inc i))
            ret))))
    (finally
      (when (compare-and-set! (:gc-running? gs) false true)
        (try
          (gc gs)
          (finally
            (reset! (:gc-running? gs) false)))))))

(defn vid-of [data]
  (let [b (byte-array 16)
        bb (ByteBuffer/wrap b)]
    (if (instance? UUID data)
      (let [^UUID data data]
        (.putLong bb (.getMostSignificantBits data))
        (.putLong bb (.getLeastSignificantBits data)))
      (do
        (.putLong bb 0)
        (.putLong bb (long data))))
    b))

(defn bytes->uuid [bytes]
  (let [bb (ByteBuffer/wrap bytes)
        m (.getLong bb)
        l (.getLong bb)]
    (UUID. m l)))

(defn vid-of-edge
  "get the raw vid bytes for an edge"
  [gs id edge]
  (let [gfs (r/as (r/t (:graph-fragments (:config gs)) :fragment_id :graph_id)
                  :gfs)
        e (r/as (r/t (:edge (:config gs)) :src :dest :vid) :e)]
    (second
     (jdbc/query
      (:con gs)
      (-> (r/⨝ gfs e (r/≡ :e/fragment_id :gfs/fragment_id))
          (r/σ #{(r/≡ :gfs/graph_id (r/lit id))
                 (r/≡ :e/src (r/lit (vid-of (g/src edge))))
                 (r/≡ :e/dest (r/lit (vid-of (g/dest edge))))})
          (r/π :e/vid))
      :as-arrays? true
      :row-fn (fn [[vid]] vid)))))

(defn cessors [gs id node prefix]
  (let [known (case prefix
                :prede :dest/vid
                :suc :src/vid)
        unknown (case prefix
                  :prede :src/vid
                  :suc :dest/vid)
        gfs (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
        n (r/t (:node (:config gs)) :vid :fragment_id)
        e (r/t (:edge (:config gs)) :src :dest :fragment_id)]
    (rest
     (jdbc/query
      (:con gs)
      (-> (r/⨝ (r/as n :src) (r/as e :e) (r/≡ :e/src
                                              :src/vid))
          (r/⨝ (r/as n :dest) (r/≡ :e/dest :dest/vid))
          (r/⨝ (r/as gfs :ef) (r/≡ :e/fragment_id
                                   :ef/fragment_id))
          (r/⨝ (r/as gfs :sf) (r/≡ :src/fragment_id
                                   :sf/fragment_id))
          (r/⨝ (r/as gfs :df) (r/≡ :dest/fragment_id
                                   :df/fragment_id))
          (r/σ #{(r/≡ :ef/graph_id (r/lit id))
                 (r/≡ :sf/graph_id (r/lit id))
                 (r/≡ :df/graph_id (r/lit id))
                 (r/≡ known (r/lit (vid-of node)))})
          (r/π unknown)
          (r/to-sql))
      :as-arrays? true
      :row-fn (fn [[id]] (bytes->uuid id))))))

(defrecord G [gs id]
  g/EditableGraph
  (add-nodes* [g nodes]
    (jdbc/with-db-transaction
      [con (:con gs) :isolation :serializable :read-only? false]
      (->G gs (reduce
               (fn [graph nodes]
                 (alloc (assoc gs :con con) graph
                        (fn [fragment-id]
                          (doseq [node nodes]
                            (when-not (g/has-node? (id-graph gs graph) node)
                              (jdbc/insert! con
                                            (:node (:config gs))
                                            {:fragment_id fragment-id
                                             :vid (vid-of node)}))))))
               id
               (partition-all 10 nodes)))))
  (add-edges* [g edges]
    (->G gs (reduce
             (fn [graph edges]
               (alloc
                gs
                graph
                (fn [fragment-id]
                  (doseq [[src target weight] edges]
                    (when-not (g/has-node? (id-graph gs graph) src)
                      (jdbc/insert! (:con gs)
                                    (:node (:config gs))
                                    {:fragment_id fragment-id
                                     :vid (vid-of src)}))
                    (when-not (g/has-node? (id-graph gs graph) target)
                      (when-not (= src target)
                        (jdbc/insert! (:con gs)
                                      (:node (:config gs))
                                      {:fragment_id fragment-id
                                       :vid (vid-of target)})))
                    (jdbc/insert! (:con gs)
                                  (:edge (:config gs))
                                  {:vid (vid-of (UUID/randomUUID))
                                   :src (vid-of src)
                                   :fragment_id fragment-id
                                   :dest (vid-of target)
                                   :weight (or weight 0)})))))
             id
             (partition-all 10 edges))))
  (remove-edges* [g edges]
    (->G gs (reduce
             (fn [gid [src dest]]
               (assert (or (number? src)
                           (instance? UUID src))
                       src)
               (assert (or (number? src)
                           (instance? UUID src))
                       dest)
               (reduce
                (fn [gid [id fid]]
                  (copy-without gs gid fid :edge id))
                gid
                (let [gfs (r/t (:graph-fragments (:config gs))
                               :id :graph_id :fragment_id)
                      e (r/t (:edge (:config gs)) :id :fragment_id :src :dest)]
                  ;; :as-arrays? returns a column header, bonkers
                  (rest (jdbc/query (:con gs)
                                    (-> (r/⨝ (r/as e :e) (r/as gfs :gfs)
                                             (r/≡ :e/fragment_id
                                                  :gfs/fragment_id))
                                        (r/σ #{(r/≡ :e/src
                                                    (r/lit (vid-of src)))
                                               (r/≡ :e/dest
                                                    (r/lit (vid-of dest)))
                                               (r/≡ :gfs/graph_id
                                                    (r/lit gid))})
                                        (r/π :e/id :e/fragment_id)
                                        (r/to-sql))
                                    :as-arrays? true)))))
             id
             edges)))
  (remove-nodes* [g nodes]
    (->G gs (reduce
             (fn [gid node]
               (reduce
                (fn [gid [id fid]]
                  (copy-without gs gid fid :node id))
                gid
                (let [gfs (r/t (:graph-fragments (:config gs))
                               :id :graph_id :fragment_id)
                      n (r/t (:node (:config gs)) :id :fragment_id :vid)]
                  (rest
                   (jdbc/query
                    (:con gs)
                    (-> (r/⨝ (r/as n :n) (r/as gfs :gfs) (r/≡ :n/fragment_id
                                                              :gfs/fragment_id))
                        (r/σ (r/∧ (r/≡ :n/vid (r/lit (vid-of node)))
                                  (r/≡ :gfs/graph_id (r/lit gid))))
                        (r/π :n/id :n/fragment_id)
                        (r/to-sql))
                    :as-arrays? true)))))
             id
             nodes)))
  g/Graph
  (nodes [this]
    (let [gfs (r/t (:graph-fragments (:config gs)) :id :graph_id :fragment_id)
          n (r/t (:node (:config gs)) :id :fragment_id :vid)]
      (rest
       (jdbc/query
        (:con gs)
        (-> (r/⨝ (r/as n :n) (r/as gfs :gfs) (r/≡ :n/fragment_id
                                                  :gfs/fragment_id))
            (r/σ (r/≡ :gfs/graph_id (r/lit id)))
            (r/π :n/vid)
            (r/to-sql))
        :as-arrays? true
        :row-fn (comp bytes->uuid first)))))
  (edges [this]
    (assert (number? id))
    (let [gfs (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
          n (r/t (:node (:config gs)) :vid :fragment_id)
          e (r/t (:edge (:config gs)) :src :dest :fragment_id :weight)]
      (rest
       (jdbc/query
        (:con gs)
        (-> (r/⨝ (r/as n :src) (r/as e :e) (r/≡ :e/src :src/vid))
            (r/⨝ (r/as n :dest) (r/≡ :e/dest :dest/vid))
            (r/⨝ (r/as gfs :ef) (r/≡ :e/fragment_id :ef/fragment_id))
            (r/⨝ (r/as gfs :sf) (r/≡ :src/fragment_id :sf/fragment_id))
            (r/⨝ (r/as gfs :df) (r/≡ :dest/fragment_id :df/fragment_id))
            (r/σ #{(r/≡ :ef/graph_id (r/lit id))
                   (r/≡ :sf/graph_id (r/lit id))
                   (r/≡ :df/graph_id (r/lit id))})
            (r/π :e/src :e/dest :e/weight)
            (r/to-sql))
        :as-arrays? true
        :row-fn (fn [[src dest weight]]
                  [(bytes->uuid src) (bytes->uuid dest) weight])))))
  (has-node? [this node]
    (let [gfs (r/t (:graph-fragments (:config gs)) :id :graph_id :fragment_id)
          n (r/t (:node (:config gs)) :id :fragment_id :vid)]
      (boolean
       (seq
        (jdbc/query
         (:con gs)
         (-> (r/⨝ (r/as n :n) (r/as gfs :gfs) (r/≡ :n/fragment_id
                                                   :gfs/fragment_id))
             (r/σ #{(r/≡ :gfs/graph_id (r/lit id))
                    (r/≡ :n/vid (r/lit (vid-of node)))})
             (r/π :n/vid)
             (r/to-sql)))))))
  (has-edge? [this src dest]
    (let [gf (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
          e (r/t (:edge (:config gs)) :id :src :dest :weight :fragment_id)]
      (boolean
       (seq
        (jdbc/query
         (:con gs)
         (-> (r/⨝ (r/as e :e) (r/as gf :gf) (r/≡ :e/fragment_id
                                                 :gf/fragment_id))
             (r/σ #{(r/≡ :gf/graph_id (r/lit id))
                    (r/≡ :e/src (r/lit (vid-of src)))
                    (r/≡ :e/dest (r/lit (vid-of dest)))})
             (r/π :e/id)
             (r/to-sql)))))))
  (successors [g]
    (partial g/successors g))
  (successors [this node]
    (cessors gs id node :suc))
  (out-degree [this node]
    (count (g/successors this node)))
  (out-edges [this node]
    (let [gf (r/as (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
                   :gf)
          e (r/as (r/t (:edge (:config gs)) :id :src :dest :weight :fragment_id)
                  :e)]
      (rest
       (jdbc/query
        (:con gs)
        (-> (r/⨝ e gf (r/≡ :e/fragment_id :gf/fragment_id))
            (r/σ #{(r/≡ :gf/graph_id (r/lit id))
                   (r/≡ :e/src (r/lit (vid-of node)))})
            (r/π :e/src :e/dest :e/weight)
            (r/to-sql))
        :as-arrays? true
        :row-fn (fn [[src dest weight]]
                  [(bytes->uuid src) (bytes->uuid dest) weight])))))
  a/AttrGraph
  (add-attr [g node-or-edge k v]
    (assert (keyword? k))
    (assert (namespace k))
    (let [type (namespace k)
          table-for-value-type (first (for [[k v] (:config gs)
                                            :when (= "attribute" (namespace k))
                                            :when (= type (name k))]
                                        v))
          object-type (if (or (number? node-or-edge)
                              (instance? UUID node-or-edge))
                        "node"
                        "edge")
          object-id (case object-type
                      "node" node-or-edge
                      ;; TODO: needs test here
                      "edge" (vid-of-edge gs id node-or-edge))]
      (->G gs (alloc gs id
                     (fn [fragment-id]
                       (jdbc/insert! (:con gs)
                                     table-for-value-type
                                     {:fragment_id fragment-id
                                      :object_type object-type
                                      :object_vid (vid-of object-id)
                                      :name (name k)
                                      :value v}))))))
  (attr [g node-or-edge k]
    (assert (keyword? k))
    (assert (namespace k))
    (let [type (namespace k)
          table-for-value-type (first (for [[k v] (:config gs)
                                            :when (= "attribute" (namespace k))
                                            :when (= type (name k))]
                                        v))
          attribute-name (name k)
          object-type (if (or (number? node-or-edge)
                              (instance? UUID node-or-edge))
                        "node"
                        "edge")
          object-id (case object-type
                      "node" (vid-of node-or-edge)
                      "edge" (vid-of-edge gs id node-or-edge))
          gfs (r/as (r/t (:graph-fragments (:config gs))
                         :fragment_id :graph_id)
                    :gfs)
          a (r/as (r/t table-for-value-type
                       :value :fragment_id :object_type :object_vid :value :id
                       :name)
                  :a)]
      (second
       (jdbc/query
        (:con gs)
        (-> (r/⨝ gfs a (r/≡ :a/fragment_id :gfs/fragment_id))
            (r/σ #{(r/≡ :gfs/graph_id (r/lit id))
                   (r/≡ :a/object_type (r/lit object-type))
                   (r/≡ :a/object_vid (r/lit object-id))
                   (r/≡ :a/name (r/lit attribute-name))})
            (r/π :a/value)
            (r/↓ :a/id)
            (r/to-sql))
        :as-arrays? true
        :row-fn (fn [[value]]
                  value)))))
  g/Digraph
  (predecessors [g]
    (partial g/predecessors g))
  (predecessors [this node]
    (cessors gs id node :prede))
  (in-degree [this node]
    (count (g/predecessors this node)))
  (in-edges [this node]
    (let [gf (r/as (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
                   :gf)
          e (r/as (r/t (:edge (:config gs)) :id :src :dest :weight :fragment_id)
                  :e)]
      (rest
       (jdbc/query
        (:con gs)
        (-> (r/⨝ e gf (r/≡ :e/fragment_id :gf/fragment_id))
            (r/σ #{(r/≡ :gf/graph_id (r/lit id))
                   (r/≡ :e/dest (r/lit (vid-of node)))})
            (r/π :e/src :e/dest :e/weight)
            (r/to-sql))
        :as-arrays? true
        :row-fn (fn [[src dest weight]]
                  [(bytes->uuid src) (bytes->uuid dest) weight])))))
  (transpose [this]
    (t/->G this))
  g/WeightedGraph
  (weight [g]
    (partial g/weight g))
  (weight [this edge]
    (g/weight this (g/src edge) (g/dest edge)))
  (weight [g n1 n2]
    (let [e (r/t (:edge (:config gs)) :vid :src :dest :id :weight :fragment_id)
          gf (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
          s (-> (r/⨝ (r/as e :e) (r/as gf :gf) (r/≡ :e/fragment_id
                                                    :gf/fragment_id))
                (r/σ #{(r/≡ :e/src (r/lit (vid-of n1)))
                       (r/≡ :e/dest (r/lit (vid-of n2)))
                       (r/≡ :gf/graph_id (r/lit id))})
                (r/π :e/weight)
                (r/to-sql))
          r (or (first (map :weight (jdbc/query (:con gs) s))) 0)]
      r)))

(defn id-graph [gs id]
  (->G gs id))
