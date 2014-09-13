(ns com.manigfeald.graph
  (:require [loom.graph :as g]
            [loom.attr :as a]
            [clojure.java.jdbc :as jdbc]
            [com.manigfeald.graph.readonly :as ro]
            [com.manigfeald.kvgc :as k]
            [com.manigfeald.graph.gc :as gc]
            [com.manigfeald.graph.alloc :refer [alloc]]
            [com.manigfeald.graph.rel :as r])
  (:import (java.util.concurrent.locks ReentrantReadWriteLock
                                       ReentrantLock)
           (java.nio ByteBuffer)
           (java.lang.ref WeakReference)
           (java.util UUID)))

;; TODO: use a reference queue

(defrecord GraphStore [con config rw gc-running? views references heap locks])

(defn gs [con config]
  (->GraphStore con config
                (ReentrantReadWriteLock.)
                (atom false)
                (atom #{})
                (atom {})
                (gc/->Heap con config (atom false))
                (atom {})))

(defn copy-without [gs prev-graph frag-id table id]
  {:pre [(number? prev-graph)
         (number? frag-id)
         (keyword? table)
         (number? id)]}
  (let [graph-id (first (vals (first (jdbc/insert! (:con gs) (:graph (:config gs)) {:x 0}))))
        _ (reduce
           (fn [_ frag]
             (if (= frag-id (:fragment_id frag))
               (let [new-frag-id (first (vals (first (jdbc/insert! (:con gs) (:fragment (:config gs)) {:size 0}))))
                     c (atom 0)]
                 (jdbc/insert! (:con gs) (:graph-fragments (:config gs)) {:graph_id graph-id :fragment_id new-frag-id})
                 (doseq [[k v] (:config gs)
                         :when (not (contains? #{:fragment :named-graph :graph :graph-fragments} k))
                         :let [qs (format "SELECT * FROM %s WHERE fragment_id = ?" v)]
                         item (jdbc/query (:con gs) [qs frag-id])
                         :when (not (and (= frag-id (:fragment_id item))
                                         (= table k)
                                         (= id (:id item))))]
                   (swap! c inc)
                   (jdbc/insert! (:con gs) v (assoc (dissoc item :id :tag)
                                               :fragment_id new-frag-id)))
                 (jdbc/update! (:con gs) (:fragment (:config gs)) {:size @c} ["id = ?" frag-id]))
               (jdbc/insert! (:con gs) (:graph-fragments (:config gs)) (dissoc (assoc frag :graph_id graph-id) :id :tag))))
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
                        (into (set (rest (jdbc/query (:con gs)
                                                     (-> (r/as (r/t (:named-graph (:config gs)) :id) :ng)
                                                         (r/π :ng/id)
                                                         (r/to-sql))
                                                     :as-arrays? true
                                                     :row-fn (fn [[id]] [:named-graph id]))))
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
      (let [g (or (first (jdbc/query (:con gs)
                                     [(format "SELECT * FROM %s WHERE name = ?" (:named-graph (:config gs)))
                                      graph-name]))
                  {:graph_id (alloc gs -1)})
            ro (ro/->ROG (id-graph gs (biginteger (:graph_id g))) (fn [_] (swap! (:views gs) disj (:graph_id g))))]
        (swap! (:references gs) (fn [m k v] (if (contains? m k) m (assoc m k v))) (:graph_id g) (WeakReference. ro))
        (swap! (:views gs) conj (:graph_id g))
        ro))))

(defmacro java-locking [lock & body]
  `(let [lock# ~lock]
     (try
       (.lock lock#)
       ~@body
       (finally
         (.unlock lock#)))))

(defn transact [gs graph-name fun]
  (try
    (with-read-lock gs
      (loop [i 0]
        (when (= i 100)
          (assert nil))
        (let [g (or (first (jdbc/query (:con gs)
                                       (-> (r/as (r/t (:named-graph (:config gs)) :graph_id :id :name) :ng)
                                           (r/σ (r/≡ :ng/name (r/lit graph-name)))
                                           (r/π :ng/id :ng/graph_id)
                                           (r/to-sql))))
                    {:graph_id (alloc gs -1)
                     :new? true})
              [ret ng] (fun (id-graph gs (biginteger (:graph_id g))))]
          (swap! (:locks gs) (fn [locks]
                               (if (contains? locks graph-name)
                                 locks
                                 (assoc locks graph-name (ReentrantLock.)))))
          (assert ng)
          (assert (:id ng))
          (assert (satisfies? g/Graph ng))
          (if-not (java-locking
                   (get (deref (:locks gs)) graph-name)
                   ;; TODO: don't need the lock at all, just cas using
                   ;; update and check for cas success
                   (let [cg (or (first (jdbc/query
                                        (:con gs)
                                        (-> (r/as (r/t (:named-graph (:config gs)) :graph_id :id :name) :ng)
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
        (.putLong bb 0)
        (.putLong bb (long data))))
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
                (let [gfs (r/t (:graph-fragments (:config gs)) :id :graph_id :fragment_id)
                      e (r/t (:edge (:config gs)) :id :fragment_id :src :dest)]
                  ;; :as-arrays? returns a column header, bonkers
                  (rest (jdbc/query (:con gs)
                                    (-> (r/⨝ (r/as e :e) (r/as gfs :gfs) (r/≡ :e/fragment_id :gfs/fragment_id))
                                        (r/σ #{(r/≡ :e/src (r/lit (vid-of src)))
                                               (r/≡ :e/dest (r/lit (vid-of dest)))
                                               (r/≡ :gfs/graph_id (r/lit gid))})
                                        (r/π :e/id :e/fragment_id)
                                        (r/to-sql))
                                    :as-arrays? true)))))
             id
             edges)))
  (remove-nodes* [g nodes]
    ;; (println "remove-nodes" nodes)
    (->G gs (reduce
             (fn [gid node]
               (reduce
                (fn [gid [id fid]]
                  (copy-without gs gid fid :node id))
                gid
                (let [gfs (r/t (:graph-fragments (:config gs)) :id :graph_id :fragment_id)
                      n (r/t (:node (:config gs)) :id :fragment_id :vid)]
                  ;; :as-arrays? returns a column header, bonkers
                  (rest (jdbc/query (:con gs)
                                    (-> (r/⨝ (r/as n :n) (r/as gfs :gfs) (r/≡ :n/fragment_id :gfs/fragment_id))
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
      (rest (jdbc/query (:con gs)
                        (-> (r/⨝ (r/as n :n) (r/as gfs :gfs) (r/≡ :n/fragment_id :gfs/fragment_id))
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
      (rest (jdbc/query (:con gs)
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
      (boolean (seq (jdbc/query (:con gs)
                                (-> (r/⨝ (r/as n :n) (r/as gfs :gfs) (r/≡ :n/fragment_id :gfs/fragment_id))
                                    (r/σ #{(r/≡ :gfs/graph_id (r/lit id))
                                           (r/≡ :n/vid (r/lit (vid-of node)))})
                                    (r/π :n/vid)
                                    (r/to-sql)))))))
  (has-edge? [this src dest]
    (let [gf (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
          e (r/t (:edge (:config gs)) :id :src :dest :weight :fragment_id)]
      (boolean (seq (jdbc/query (:con gs)
                                (-> (r/⨝ (r/as e :e) (r/as gf :gf) (r/≡ :e/fragment_id :gf/fragment_id))
                                    (r/σ #{(r/≡ :gf/graph_id (r/lit id))
                                           (r/≡ :e/src (r/lit (vid-of src)))
                                           (r/≡ :e/dest (r/lit (vid-of dest)))})
                                    (r/π :e/id)
                                    (r/to-sql)))))))
  (successors [g]
    (partial g/successors g))
  (successors [this node]
    (let [gfs (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
          n (r/t (:node (:config gs)) :vid :fragment_id)
          e (r/t (:edge (:config gs)) :src :dest :fragment_id)]
      (rest (jdbc/query (:con gs)
                        (-> (r/⨝ (r/as n :src) (r/as e :e) (r/≡ :e/src :src/vid))
                            (r/⨝ (r/as n :dest) (r/≡ :e/dest :dest/vid))
                            (r/⨝ (r/as gfs :ef) (r/≡ :e/fragment_id :ef/fragment_id))
                            (r/⨝ (r/as gfs :sf) (r/≡ :src/fragment_id :sf/fragment_id))
                            (r/⨝ (r/as gfs :df) (r/≡ :dest/fragment_id :df/fragment_id))
                            (r/σ #{(r/≡ :ef/graph_id (r/lit id))
                                   (r/≡ :sf/graph_id (r/lit id))
                                   (r/≡ :df/graph_id (r/lit id))
                                   (r/≡ :src/vid (r/lit (vid-of node)))})
                            (r/π :e/dest)
                            (r/to-sql))
                        :as-arrays? true
                        :row-fn (fn [[id]]
                                  (bytes->uuid id))))))
  (out-degree [this node]
    (count (g/successors this node)))
  (out-edges [this node]
    (let [gf (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
          e (r/t (:edge (:config gs)) :id :src :dest :weight :fragment_id)]
      (rest (jdbc/query (:con gs)
                        (-> (r/⨝ (r/as e :e) (r/as gf :gf) (r/≡ :e/fragment_id :gf/fragment_id))
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
                      "edge"
                      (:vid (first (jdbc/query (:con gs) [(format "
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
                                                          (g/src node-or-edge)
                                                          (g/dest node-or-edge)]))))]
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
    ;; check for node existence
    (let [gfs (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
          n (r/t (:node (:config gs)) :vid :fragment_id)
          e (r/t (:edge (:config gs)) :src :dest :fragment_id)]
      (rest (jdbc/query (:con gs)
                        (-> (r/⨝ (r/as n :src) (r/as e :e) (r/≡ :e/src :src/vid))
                            (r/⨝ (r/as n :dest) (r/≡ :e/dest :dest/vid))
                            (r/⨝ (r/as gfs :ef) (r/≡ :e/fragment_id :ef/fragment_id))
                            (r/⨝ (r/as gfs :sf) (r/≡ :src/fragment_id :sf/fragment_id))
                            (r/⨝ (r/as gfs :df) (r/≡ :dest/fragment_id :df/fragment_id))
                            (r/σ #{(r/≡ :ef/graph_id (r/lit id))
                                   (r/≡ :sf/graph_id (r/lit id))
                                   (r/≡ :df/graph_id (r/lit id))
                                   (r/≡ :dest/vid (r/lit (vid-of node)))})
                            (r/π :e/src)
                            (r/to-sql))
                        :as-arrays? true
                        :row-fn (fn [[id]]
                                  (bytes->uuid id))))))
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
    (let [e (r/t (:edge (:config gs)) :vid :src :dest :id :weight :fragment_id)
          gf (r/t (:graph-fragments (:config gs)) :graph_id :fragment_id)
          s (-> (r/⨝ (r/as e :e) (r/as gf :gf) (r/≡ :e/fragment_id :gf/fragment_id))
                (r/σ #{(r/≡ :e/src (r/lit (vid-of n1)))
                       (r/≡ :e/dest (r/lit (vid-of n2)))
                       (r/≡ :gf/graph_id (r/lit id))})
                (r/π :e/weight)
                (r/to-sql))
          r (or (first (map :weight (jdbc/query (:con gs) s))) 0)]
      r)))

(defn id-graph [gs id]
  (->G gs id))
