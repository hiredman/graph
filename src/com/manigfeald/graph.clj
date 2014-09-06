(ns com.manigfeald.graph
  (:require [loom.graph :as g]
            [loom.attr :as a]
            [clojure.java.jdbc :as jdbc]
            [com.manigfeald.graph.node :as n]
            [com.manigfeald.graph.readonly :as ro]
            [com.manigfeald.kvgc :as k])
  (:import (java.util.concurrent.locks ReentrantReadWriteLock)
           (java.lang.ref WeakReference)))

;; TODO: this is insane

(defrecord GraphStore [con config rw gc-running? views references mark-state]
  k/Heap
  (references [heap ptr]
    (let [[table id] ptr
          _ (assert table)
          _ (assert id)
          x (case table
              :named-graph (for [{:keys [graph_id]}
                                 (jdbc/query con [(format "SELECT graph_id FROM %s WHERE id = ?" (:named-graph config)) id])]
                             [:graph graph_id])
              :graph (for [{:keys [id]}
                           (jdbc/query con [(format "SELECT id FROM %s WHERE graph_id = ?" (:graph-fragments config)) id])]
                       [:graph-fragments id])
              :fragment (for [[k v] config
                              :when (not (contains? #{:named-graph :graph :fragment :graph-fragments} k))
                              {:keys [id]} (jdbc/query con [(format "SELECT id FROM %s WHERE fragment_id = ?" (k config)) id])]
                          [k id])
              :graph-fragments (for [{:keys [fragment_id graph_id]}
                                     (jdbc/query
                                      con
                                      [(format "SELECT graph_id,fragment_id FROM %s WHERE id = ?" (:graph-fragments config)) id])
                                     i [[:fragment fragment_id]
                                        [:graph graph_id]]]
                                 i)
              :edge (for [{:keys [src dest fragment_id]}
                          (jdbc/query con [(format "SELECT src,dest,fragment_id FROM %s WHERE id = ?" (:edge config)) id])
                          n [[:node src]
                             [:node dest]
                             [:fragment fragment_id]]]
                      n)
              :node (for [{:keys [fragment_id]} (jdbc/query con [(format "SELECT fragment_id FROM %s WHERE id = ?" (:node config)) id])]
                      [:fragment fragment_id])
              (if (= "attribute" (namespace table))
                (for [{:keys [object_type object_id  fragment_id]}
                      (jdbc/query con [(format "SELECT object_type, object_id, fragment_id FROM %s WHERE id = ?" (table config)) id])
                      i [[(keyword object_type) object_id]
                         [:fragment fragment_id]]]
                  i)
                (assert nil [table ptr])))]
      (assert (not (seq (for [[k v] x
                              :when (or (nil? k) (nil? v))]
                          true))))
      x))
  (tag [heap ptr tag-value]
    (let [tag-value (case tag-value
                      true 1
                      false 0)
          [table id] ptr
          table-name (get config table)]
      (jdbc/update! con table-name {:tag tag-value} ["id = ?" id])
      heap))
  (tag-value [heap ptr]
    (let [[table id] ptr
          table-name (get config table)]
      (case (:tag (first (jdbc/query con [(format "SELECT tag FROM %s WHERE id = ?" table-name) id])))
        nil true
        1 true
        0 false)))
  (free [heap ptr]
    (let [[table id] ptr
          table-name (get config table)]
      (jdbc/delete! con table-name ["id = ?" id])
      heap))
  (with-heap-lock [heap fun]
    (fun heap))
  clojure.core.protocols/CollReduce
  (coll-reduce [_ fun]
    (reduce fun
            (for [[k v] config
                  i (case k
                      :named-graph (map
                                    (juxt (constantly k) :id)
                                    (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :graph (map
                              (juxt (constantly k) :id)
                              (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :fragment (map
                                 (juxt (constantly k) :id)
                                 (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :graph-fragments (map
                                        (juxt (constantly k) :id)
                                        (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :edge (map
                             (juxt (constantly k) :id)
                             (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :node (map
                             (juxt (constantly k) :id)
                             (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      (if (= "attribute" (namespace k))
                        (map
                         (juxt (constantly k) :id)
                         (jdbc/query con [(format "SELECT id FROM %s" v)]))
                        (assert nil [k v])))]
              i)))
  (coll-reduce [_ fun init]
    (reduce fun
            init
            (for [[k v] config
                  i (case k
                      :named-graph (map
                                    (juxt (constantly k) :id)
                                    (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :graph (map
                              (juxt (constantly k) :id)
                              (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :fragment (map
                                 (juxt (constantly k) :id)
                                 (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :graph-fragments (map
                                        (juxt (constantly k) :id)
                                        (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :edge (map
                             (juxt (constantly k) :id)
                             (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      :node (map
                             (juxt (constantly k) :id)
                             (jdbc/query con [(format "SELECT id FROM %s" v)]))
                      (if (= "attribute" (namespace k))
                        (map
                         (juxt (constantly k) :id)
                         (jdbc/query con [(format "SELECT id FROM %s" v)]))
                        (assert nil [k v])))]
              [i nil]))))

(defn gs [con config]
  (->GraphStore con config (ReentrantReadWriteLock.) (atom false) (atom #{}) (atom {}) (atom false)))

(defn allocate-graph
  ([gs]
     (let [x (jdbc/insert! (:con gs) (:graph (:config gs)) {:x 0})]
       (biginteger (first (vals (first x))))))
  ([gs prev-graph]
     (let [graph-id (first (vals (first (jdbc/insert! (:con gs) (:graph (:config gs)) {:x 0}))))]
       (doseq [frag (jdbc/query (:con gs) [(format "SELECT * FROM %s WHERE graph_id = ?"
                                                   (:graph-fragments (:config gs)))
                                           prev-graph])]
         (jdbc/insert! (:con gs) (:graph-fragments (:config gs))
                       (dissoc (assoc frag :graph_id graph-id) :id)))
       (biginteger graph-id))))

(defn allocate-fragment [gs graph-id]
  (let [frag-id (first (vals (first (jdbc/insert! (:con gs) (:fragment (:config gs)) {:x 0}))))]
    (jdbc/insert! (:con gs) (:graph-fragments (:config gs)) {:graph_id graph-id :fragment_id frag-id})
    (biginteger frag-id)))

(def text "varchar(1024)")

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
              :fragment (jdbc/create-table-ddl v [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"] [:size :int]
                                               [:x :int]
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
                     [:src :int]
                     [:dest :int]
                     [:weight :int]
                     [:tag :int])
              :node (jdbc/create-table-ddl
                     v
                     [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                     [:fragment_id :int]
                     [:tag :int])
              (if (= "attribute" (namespace k))
                (jdbc/create-table-ddl
                 v
                 [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
                 [:fragment_id :int]
                 [:object_type text]
                 [:object_id :int]
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
    (let [[_ nms] (k/gc gs (into (set (map (juxt (constantly :named-graph) :id)
                                           (jdbc/query (:con gs) [(format "SELECT id FROM %s" (:named-graph (:config gs)))])))
                                 (for [i @(:views gs)]
                                   [:graph i]))
                        @(:mark-state gs))]
      (reset! (:mark-state gs) nms))
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
                  {:graph_id (allocate-graph gs)})
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
                    {:graph_id (allocate-graph gs)
                     :new? true})
              [ret ng] (fun (id-graph gs (biginteger (:graph_id g))))]
          (if-not (locking gs
                    (let [cg (or (first (jdbc/query
                                         (:con gs)
                                         [(format "SELECT * FROM %s WHERE id = ?" (:named-graph (:config gs)))
                                          (:id g)]))
                                 {:graph_id (:graph_id g)})]
                      (if (= (:graph_id cg) (:graph_id g))
                        (do
                          ;; TODO: fragment compression
                          (if (:new? g)
                            (do
                              (jdbc/insert! (:con gs) (:named-graph (:config gs))
                                            {:graph_id (:id ng)
                                             :name graph-name}))
                            (do
                              (assert (:id g))
                              (jdbc/update! (:con gs)
                                            (:named-graph (:config gs))
                                            {:graph_id (:id ng)}
                                            ["id = ?" (:id g)])))
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

(defrecord G [gs id]
  n/HasNodes
  (allocate-nodes [g n]
    (let [[graph-id nodes]
          (reduce
           (fn [[graph nodes] _]
             (let [graph (allocate-graph gs graph)
                   fragment-id (allocate-fragment gs graph)
                   node-id
                   (first (vals (first (jdbc/insert! (:con gs)
                                                     (:node (:config gs))
                                                     {:fragment_id fragment-id}))))]
               [graph (conj nodes (biginteger node-id))]))
           [id []]
           (repeat n :foo))]
      [nodes (->G gs graph-id)]))
  g/EditableGraph
  (add-edges* [g edges]
    (->G gs (reduce
             (fn [graph [src target weight]]
               (let [graph (allocate-graph gs graph)
                     fragment-id (allocate-fragment gs graph)]
                 (jdbc/insert! (:con gs)
                               (:edge (:config gs))
                               {:src src
                                :fragment_id fragment-id
                                :dest target
                                :weight (or weight 0)})
                 graph))
             id
             edges)))

  g/Graph
  (nodes [this]
    (mapv :id (jdbc/query (:con gs) [(format "
SELECT %s.id AS id FROM %s
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
    (mapv (juxt :src :dest :weight)
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
  AND %s.id = ?
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
                                         id node]))))
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
                                         id src dest]))))
  ;; TODO:
  (successors [g]
    (partial g/successors g))
  (successors [this node]
    (mapv :dest (jdbc/query (:con gs) [(format "
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
                                       id node])))
  (out-degree [this node]
    (count (g/successors this node)))
  (out-edges [this node]
    (mapv (juxt :src :dest :weight)
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
                                 id node])))
  a/AttrGraph
  (add-attr [g node-or-edge k v]
    (assert (keyword? k))
    (assert (namespace k))
    (let [type (namespace k)
          table-for-value-type (first (for [[k v] (:config gs)
                                            :when (= "attribute" (namespace k))
                                            :when (= type (name k))]
                                        v))
          object-type (if (number? node-or-edge)
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
      (->G gs (let [graph (allocate-graph gs id)
                    fragment-id (allocate-fragment gs graph)]
                (jdbc/insert! (:con gs)
                              table-for-value-type
                              {:fragment_id fragment-id
                               :object_type object-type
                               :object_id object-id
                               :name (name k)
                               :value v})
                graph))))
  (attr [g node-or-edge k]
    (assert (keyword? k))
    (assert (namespace k))
    (let [type (namespace k)
          table-for-value-type (first (for [[k v] (:config gs)
                                            :when (= "attribute" (namespace k))
                                            :when (= type (name k))]
                                        v))
          object-type (if (number? node-or-edge)
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
  WHERE object_id = ?
  AND object_type = ?
  AND graph_id = ?"
                                                    table-for-value-type
                                                    (:graph-fragments (:config gs))
                                                    (:graph-fragments (:config gs))
                                                    table-for-value-type
                                                    (:graph (:config gs))
                                                    (:graph-fragments (:config gs))
                                                    (:graph (:config gs)))
                                            object-id
                                            object-type
                                            id])))))
  g/Digraph
  (predecessors [g]
    (partial g/predecessors g))
  (predecessors [this node]
    (mapv :src (jdbc/query (:con gs) [(format "
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
                                      id node])))
  (in-degree [this node]
    (count (g/predecessors this node)))
  (in-edges [this node]
    (mapv (juxt :src :dest :weight)
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
                                 id node])))
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
                                            n1
                                            n2])))
        0)))

(defn id-graph [gs id]
  (->G gs id))

(defrecord ROG [g]
  g/Graph
  (nodes [this]
    (g/nodes g))
  (edges [this]
    (g/edges g))
  (has-node? [this node]
    (g/has-node? g node))
  (has-edge? [this src dest]
    (g/has-edge? g src dest))
  (successors [g]
    (partial g/successors g))
  (successors [this node]
    (g/successors g node))
  (out-degree [this node]
    (g/out-degree g node))
  (out-edges [this node]
    (g/out-edges g node))
  a/AttrGraph
  (attr [_ node-or-edge k]
    (a/attr g node-or-edge k))
  g/Digraph
  (predecessors [g]
    (partial g/predecessors g))
  (predecessors [this node]
    (g/predecessors g node))
  (in-degree [this node]
    (g/in-degree g node))
  (in-edges [this node]
    (g/in-edges g node))
  (transpose [this]
    (assert nil))
  g/WeightedGraph
  (weight [g]
    (partial g/weight g))
  (weight [this edge]
    (g/weight this (g/src edge) (g/dest edge)))
  (weight [_ n1 n2]
    (g/weight g n1 n2)))
