(ns com.manigfeald.graph
  (:require [loom.graph :as g]
            [loom.attr :as a]
            [clojure.java.jdbc :as jdbc]
            [com.manigfeald.graph.readonly :as ro]
            [com.manigfeald.kvgc :as k]
            [com.manigfeald.graph.gc :as gc]
            [com.manigfeald.graph.alloc :refer [alloc]])
  (:import (java.util.concurrent.locks ReentrantReadWriteLock)
           (java.nio ByteBuffer)
           (java.lang.ref WeakReference)
           (java.util UUID)))

(defrecord GraphStore [con config rw gc-running? views references heap])

(defn gs [con config]
  (->GraphStore con config
                (ReentrantReadWriteLock.)
                (atom false)
                (atom #{})
                (atom {})
                (gc/->Heap con config (atom false))))

(defn copy-without [gs prev-graph frag-id table id]
  {:pre [(number? prev-graph)
         (number? frag-id)
         (keyword? table)
         (number? id)]}
  (let [graph-id (first (vals (first (jdbc/insert! (:con gs) (:graph (:config gs)) {:x 0}))))
        new-frag-id (reduce
                     (fn [id frag]
                       (if (= frag-id (:fragment_id frag))
                         (let [frag-id (first (vals (first (jdbc/insert! (:con gs) (:fragment (:config gs)) {:size 0}))))
                               c (atom 0)]
                           (jdbc/insert! (:con gs) (:graph-fragments (:config gs)) {:graph_id graph-id :fragment_id frag-id})
                           (doseq [[k v] (:config gs)
                                   :when (not (contains? #{:fragment :named-graph :graph} k))
                                   item (jdbc/query (:con gs) [(format "SELECT * FROM %s WHERE fragment_id = ?" v) frag-id])
                                   :when (not (and (= frag-id (:fragment_id item))
                                                   (= table k)
                                                   (= id (:id item))))]
                             (swap! c inc)
                             (first (vals (first (jdbc/insert! (:con gs) v (assoc (dissoc item :id :tag)
                                                                             :fragment_id frag-id))))))
                           (jdbc/update! (:con gs) (:fragment (:config gs)) {:size @c} ["id = ?" frag-id]))
                         (do
                           (jdbc/insert! (:con gs) (:graph-fragments (:config gs)) (dissoc (assoc frag :graph_id graph-id) :id :tag))
                           id)))
                     nil
                     (jdbc/query (:con gs) [(format "SELECT * FROM %s WHERE graph_id = ?"
                                                    (:graph-fragments (:config gs)))
                                            prev-graph]))]
    (biginteger graph-id)))

(def text "varchar(1024)")
(def vid "CHAR(16) FOR BIT DATA")

(defn create-tables! [gs]
  ;; TODO: add indices
  (let [x (for[[k v] (:config gs)]
            (case k
              :named-graph (jdbc/create-table-ddl
                            v
                            [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                            [:graph_id :int]
                            [:name text]
                            [:tag :int])
              :graph (jdbc/create-table-ddl v
                                            [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                                            [:x :int]
                                            [:tag :int])
              :fragment (jdbc/create-table-ddl v [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                                               [:size :int]
                                               [:tag :int])
              :graph-fragments (jdbc/create-table-ddl
                                v
                                [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                                [:graph_id :int]
                                [:fragment_id :int]
                                [:tag :int])
              :edge (jdbc/create-table-ddl
                     v
                     [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                     [:fragment_id :int]
                     [:vid vid]
                     [:src vid]
                     [:dest vid]
                     [:weight :int]
                     [:tag :int])
              :node (jdbc/create-table-ddl
                     v
                     [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                     [:fragment_id :int]
                     [:vid vid]
                     [:tag :int])
              (if (= "attribute" (namespace k))
                (jdbc/create-table-ddl
                 v
                 [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                 [:fragment_id :int]
                 [:object_type text]
                 [:object_vid vid]
                 [:name text]
                 [:value (let [x (keyword (name k))]
                           (if (= x :text)
                             text
                             x))]
                 [:tag :int])
                (assert nil [k v]))))]
    (apply jdbc/db-do-commands (:con gs) x)))

(defn gc [gs]
  (try
    (.lock (.writeLock (:rw gs)))
    (doseq [[graph-id wr] @(:references gs)
            :when (not (.get wr))]
      (swap! (:views gs) disj graph-id)
      (swap! (:references gs) dissoc graph-id))
    (let [[_ nms] (k/gc (:heap gs)
                        (into (set (map (juxt (constantly :named-graph) :id)
                                        (jdbc/query (:con gs)
                                                    [(format "SELECT id FROM %s" (:named-graph (:config gs)))])))
                              (for [i @(:views gs)]
                                [:graph i]))
                        @(:mark-state (:heap gs)))]
      (reset! (:mark-state (:heap gs)) nms))
    nil
    (finally
      (.unlock (.writeLock (:rw gs))))))

(defmacro with-read-lock [gs & body]
  `(let [gs# ~gs]
     (try
       (.lock (.readLock (:rw gs#)))
       ~@body
       (finally
         (.unlock (.readLock (:rw gs#)))))))

(declare id-graph)

(defn read-only-view [gs graph-name]
  (with-read-lock gs
    (loop [i 0]
      (when (= i 100)
        (assert nil))
      (let [g (or (first (jdbc/query (:con gs) [(format "SELECT * FROM %s WHERE name = ?" (:named-graph (:config gs)))
                                                graph-name]))
                  {:graph_id (alloc gs -1)})
            ro (ro/->ROG (id-graph gs (biginteger (:graph_id g)))
                         (fn [_] (swap! (:views gs) disj (:graph_id g))))]
        (swap! (:views gs) conj (:graph_id g))
        (swap! (:references gs) (fn [m k v] (if (contains? m k) m (assoc m k v))) (:graph_id g) (WeakReference. ro))
        ro))))

(defn transact [gs graph-name fun]
  (try
    (with-read-lock gs
      (loop [i 0]
        (when (= i 100)
          (assert nil))
        (let [g (or (first (jdbc/query (:con gs) [(format "SELECT * FROM %s WHERE name = ?" (:named-graph (:config gs)))
                                                  graph-name]))
                    {:graph_id (alloc gs -1)
                     :new? true})
              [ret ng] (fun (id-graph gs (biginteger (:graph_id g))))]
          (assert ng)
          ;; TODO: per graph lock here
          (if-not (locking gs
                    (let [cg (or (first (jdbc/query
                                         (:con gs)
                                         [(format "SELECT * FROM %s WHERE id = ?" (:named-graph (:config gs)))
                                          (:id g)]))
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
      (do
        (.putLong bb (.getMostSignificantBits ^UUID data))
        (.putLong bb (.getLeastSignificantBits ^UUID data)))
      (do
        (.putLong bb (long data))
        (.putLong bb 0)))
    b))

(defn bytes->uuid [bytes]
  (let [bb (ByteBuffer/wrap bytes)
        m (.getLong bb)
        l (.getLong bb)]
    (UUID. m l)))

(defrecord G [gs id]
  g/EditableGraph
  (add-nodes* [g nodes]
    (jdbc/with-db-transaction [con (:con gs) :isolation :serializable :read-only? false]
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
               (alloc gs graph (fn [fragment-id]
                                 (doseq [[src target weight] edges]
                                   (when-not (g/has-node? (id-graph gs graph) src)
                                     (jdbc/insert! (:con gs)
                                                   (:node (:config gs))
                                                   {:fragment_id fragment-id
                                                    :vid (vid-of src)}))
                                   (when-not (g/has-node? (id-graph gs graph) target)
                                     (jdbc/insert! (:con gs)
                                                   (:node (:config gs))
                                                   {:fragment_id fragment-id
                                                    :vid (vid-of target)}))
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
                (fn [gid {:keys [id fid]}]
                  (copy-without gs gid fid :edge id))
                gid
                (jdbc/query (:con gs) [(format "
SELECT %s.id, %s.fragment_id AS fid FROM %s
  JOIN %s ON %s.fragment_id = %s.fragment_id
  WHERE src = ?
  AND dest = ?
  AND %s.graph_id = ?
"
                                               (:edge (:config gs))
                                               (:edge (:config gs))
                                               (:edge (:config gs))
                                               (:graph-fragments (:config gs))
                                               (:graph-fragments (:config gs))
                                               (:edge (:config gs))
                                               (:graph-fragments (:config gs)))
                                       (vid-of src)
                                       (vid-of dest)
                                       gid])))
             id
             edges)))
  (remove-nodes* [g nodes]
    (->G gs (reduce
             (fn [gid node]
               (reduce
                (fn [gid {:keys [id fid]}]
                  (copy-without gs gid fid :node id))
                gid
                (jdbc/query (:con gs) [(format "
SELECT %s.id, %s.fragment_id AS fid FROM %s
  JOIN %s ON %s.fragment_id = %s.fragment_id
  WHERE vid = ?
  AND %s.graph_id = ?
"
                                               (:node (:config gs))
                                               (:node (:config gs))
                                               (:node (:config gs))
                                               (:graph-fragments (:config gs))
                                               (:graph-fragments (:config gs))
                                               (:node (:config gs))
                                               (:graph-fragments (:config gs)))
                                       (vid-of node)
                                       gid])))
             id
             nodes)))
  g/Graph
  (nodes [this]
    (mapv (comp bytes->uuid :id) (jdbc/query (:con gs) [(format "
SELECT %s.vid AS id FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
"
                                                                (:node (:config gs))
                                                                (:node (:config gs))
                                                                (:fragment (:config gs))
                                                                (:node (:config gs))
                                                                (:fragment (:config gs))
                                                                (:graph-fragments (:config gs))
                                                                (:graph-fragments (:config gs))
                                                                (:fragment (:config gs))
                                                                (:graph (:config gs))
                                                                (:graph (:config gs))
                                                                (:graph-fragments (:config gs))
                                                                (:graph (:config gs)))
                                                        id])))
  (edges [this]
    (assert (number? id))
    (mapv (juxt (comp bytes->uuid :src)
                (comp bytes->uuid :dest)
                :weight)
          (jdbc/query (:con gs) [(format "
SELECT src,dest,weight FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
"
                                         (:edge (:config gs))
                                         (:fragment (:config gs))
                                         (:edge (:config gs))
                                         (:fragment (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:fragment (:config gs))
                                         (:graph (:config gs))
                                         (:graph (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:graph (:config gs)))
                                 id])))
  (has-node? [this node]
    (boolean (seq (jdbc/query (:con gs) [(format "
SELECT %s.id AS id FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.vid = ?
"
                                                 (:node (:config gs))
                                                 (:node (:config gs))
                                                 (:fragment (:config gs))
                                                 (:node (:config gs))
                                                 (:fragment (:config gs))
                                                 (:graph-fragments (:config gs))
                                                 (:graph-fragments (:config gs))
                                                 (:fragment (:config gs))
                                                 (:graph (:config gs))
                                                 (:graph (:config gs))
                                                 (:graph-fragments (:config gs))
                                                 (:graph (:config gs))
                                                 (:node (:config gs)))
                                         id (vid-of node)]))))
  (has-edge? [this src dest]
    (boolean (seq (jdbc/query (:con gs) [(format "
SELECT %s.id AS id FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.src = ?
  AND %s.dest = ?
"
                                                 (:edge (:config gs))
                                                 (:edge (:config gs))
                                                 (:fragment (:config gs))
                                                 (:edge (:config gs))
                                                 (:fragment (:config gs))
                                                 (:graph-fragments (:config gs))
                                                 (:graph-fragments (:config gs))
                                                 (:fragment (:config gs))
                                                 (:graph (:config gs))
                                                 (:graph (:config gs))
                                                 (:graph-fragments (:config gs))
                                                 (:graph (:config gs))
                                                 (:edge (:config gs))
                                                 (:edge (:config gs)))
                                         id (vid-of src) (vid-of dest)]))))
  ;; TODO:
  (successors [g]
    (partial g/successors g))
  (successors [this node]
    (mapv (comp bytes->uuid :dest) (jdbc/query (:con gs) [(format "
SELECT DISTINCT %s.dest FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.src = ?
"
                                                                  (:edge (:config gs))
                                                                  (:edge (:config gs))
                                                                  (:fragment (:config gs))
                                                                  (:edge (:config gs))
                                                                  (:fragment (:config gs))
                                                                  (:graph-fragments (:config gs))
                                                                  (:graph-fragments (:config gs))
                                                                  (:fragment (:config gs))
                                                                  (:graph (:config gs))
                                                                  (:graph (:config gs))
                                                                  (:graph-fragments (:config gs))
                                                                  (:graph (:config gs))
                                                                  (:edge (:config gs)))
                                                          id (vid-of node)])))
  (out-degree [this node]
    (count (g/successors this node)))
  (out-edges [this node]
    (mapv (juxt (comp bytes->uuid :src)
                (comp bytes->uuid :dest)
                :weight)
          (jdbc/query (:con gs) [(format "
SELECT %s.src, %s.dest, %s.weight FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.src = ?
"
                                         (:edge (:config gs))
                                         (:edge (:config gs))
                                         (:edge (:config gs))
                                         (:edge (:config gs))
                                         (:fragment (:config gs))
                                         (:edge (:config gs))
                                         (:fragment (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:fragment (:config gs))
                                         (:graph (:config gs))
                                         (:graph (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:graph (:config gs))
                                         (:edge (:config gs)))
                                 id (vid-of node)])))
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
                      "edge" (:vid (first (jdbc/query (:con gs) [(format "
SELECT %s.vid FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.src = ?
  AND %s.dest = ?
"
                                                                         (:edge (:config gs))
                                                                         (:edge (:config gs))
                                                                         (:fragment (:config gs))
                                                                         (:edge (:config gs))
                                                                         (:fragment (:config gs))
                                                                         (:graph-fragments (:config gs))
                                                                         (:graph-fragments (:config gs))
                                                                         (:fragment (:config gs))
                                                                         (:graph (:config gs))
                                                                         (:graph (:config gs))
                                                                         (:graph-fragments (:config gs))
                                                                         (:graph (:config gs))
                                                                         (:edge (:config gs))
                                                                         (:edge (:config gs)))
                                                                 id
                                                                 ]))))]
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
          object-type (if (or (number? node-or-edge)
                              (instance? UUID node-or-edge))
                        "node"
                        "edge")
          object-id (case object-type
                      "node" node-or-edge
                      "edge" (:id (first (jdbc/query (:con gs) [(format "
SELECT %s.id FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.src = ?
  AND %s.dest = ?
"
                                                                        (:edge (:config gs))
                                                                        (:edge (:config gs))
                                                                        (:fragment (:config gs))
                                                                        (:edge (:config gs))
                                                                        (:fragment (:config gs))
                                                                        (:graph-fragments (:config gs))
                                                                        (:graph-fragments (:config gs))
                                                                        (:fragment (:config gs))
                                                                        (:graph (:config gs))
                                                                        (:graph (:config gs))
                                                                        (:graph-fragments (:config gs))
                                                                        (:graph (:config gs))
                                                                        (:edge (:config gs))
                                                                        (:edge (:config gs)))
                                                                id]))))]
      (:value (first (jdbc/query (:con gs) [(format "
SELECT value FROM %s
  JOIN %s ON %s.fragment_id = %s.fragment_id
  JOIN %s ON %s.graph_id = %s.id
  WHERE object_vid = ?
  AND object_type = ?
  AND graph_id = ?"
                                                    table-for-value-type
                                                    (:graph-fragments (:config gs))
                                                    (:graph-fragments (:config gs))
                                                    table-for-value-type
                                                    (:graph (:config gs))
                                                    (:graph-fragments (:config gs))
                                                    (:graph (:config gs)))
                                            (vid-of object-id)
                                            object-type
                                            id])))))
  g/Digraph
  (predecessors [g]
    (partial g/predecessors g))
  (predecessors [this node]
    (mapv (comp bytes->uuid :src) (jdbc/query (:con gs) [(format "
SELECT DISTINCT %s.src FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.dest = ?
"
                                                                 (:edge (:config gs))
                                                                 (:edge (:config gs))
                                                                 (:fragment (:config gs))
                                                                 (:edge (:config gs))
                                                                 (:fragment (:config gs))
                                                                 (:graph-fragments (:config gs))
                                                                 (:graph-fragments (:config gs))
                                                                 (:fragment (:config gs))
                                                                 (:graph (:config gs))
                                                                 (:graph (:config gs))
                                                                 (:graph-fragments (:config gs))
                                                                 (:graph (:config gs))
                                                                 (:edge (:config gs)))
                                                         id (vid-of node)])))
  (in-degree [this node]
    (count (g/predecessors this node)))
  (in-edges [this node]
    (mapv (juxt (comp bytes->uuid :src)
                (comp bytes->uuid :dest)
                :weight)
          (jdbc/query (:con gs) [(format "
SELECT %s.src, %s.dest, %s.weight FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.dest = ?
"
                                         (:edge (:config gs))
                                         (:edge (:config gs))
                                         (:edge (:config gs))
                                         (:edge (:config gs))
                                         (:fragment (:config gs))
                                         (:edge (:config gs))
                                         (:fragment (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:fragment (:config gs))
                                         (:graph (:config gs))
                                         (:graph (:config gs))
                                         (:graph-fragments (:config gs))
                                         (:graph (:config gs))
                                         (:edge (:config gs)))
                                 id (vid-of node)])))
  (transpose [this]
    (assert nil))
  g/WeightedGraph
  (weight [g]
    (partial g/weight g))
  (weight [this edge]
    (g/weight this (g/src edge) (g/dest edge)))
  (weight [g n1 n2]
    (or (first (mapv :weight
                     (jdbc/query (:con gs) [(format "
SELECT %s.weight FROM %s
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.fragment_id = %s.id
  JOIN %s ON %s.id = %s.graph_id
  WHERE %s.id = ?
  AND %s.src = ?
  AND %s.dest = ?
"
                                                    (:edge (:config gs))
                                                    (:edge (:config gs))
                                                    (:fragment (:config gs))
                                                    (:edge (:config gs))
                                                    (:fragment (:config gs))
                                                    (:graph-fragments (:config gs))
                                                    (:graph-fragments (:config gs))
                                                    (:fragment (:config gs))
                                                    (:graph (:config gs))
                                                    (:graph (:config gs))
                                                    (:graph-fragments (:config gs))
                                                    (:graph (:config gs))
                                                    (:edge (:config gs))
                                                    (:edge (:config gs)))
                                            id
                                            (vid-of n1)
                                            (vid-of n2)])))
        0)))

(defn id-graph [gs id]
  (->G gs id))
