(ns com.manigfeald.graph.rel)

(defprotocol SQLAble
  (-to-sql [_])
  (columns [_]))

(declare sql-and)

(extend-protocol SQLAble
  clojure.lang.Keyword
  (-to-sql [this]
    [(str (name (namespace this)) "." (name this)) []])
  (columns [this]
    [this])
  clojure.lang.IPersistentSet
  (-to-sql [this]
    (-to-sql (apply sql-and this)))
  (columns [this]
    (columns (apply sql-and this))))

(defn sql-count [col]
  (reify
    SQLAble
    (-to-sql [_]
      (let [[s v] (-to-sql col)]
        [(str "COUNT(" s ")") v]))
    (columns [_]
      (columns col))))

(defn binary-operator [op a b]
  (reify
    SQLAble
    (-to-sql [_]
      (let [[s v] (-to-sql a)
            [s1 v1] (-to-sql b)]
        [(str "(" s " " op " " s1 ")")
         (into v v1)]))
    (columns [_]
      (concat (columns a)
              (columns b)))))

(defn equals [a b]
  (binary-operator "=" a b))

;; (defn ≍ [a b]
;;   (equals a b))

(defn ≡ [a b]
  (equals a b))

(defn sql-and
  ([a b]
     (binary-operator "AND" a b))
  ([a b & more]
     (apply sql-and (sql-and a b) more)))

(defn sql-or
  ([a b]
     (binary-operator "OR" a b))
  ([a b & more]
     (apply sql-and (sql-and a b) more)))

(defn ∧ [& args]
  (apply sql-and args))

(defn ∨ [& args]
  (apply sql-or args))

(defrecord Union [a b])

(defrecord Table [the-name columns]
  SQLAble
  (-to-sql [_] [(name the-name) []])
  (columns [_]
    (for [column columns]
      (keyword (name the-name)
               (name column)))))

(defrecord As [table new-name]
  SQLAble
  (-to-sql [_]
    (let [[s v] (-to-sql table)]
      [(str s " AS " (name new-name)) v]))
  (columns [_]
    (for [column (columns table)]
      (keyword (name new-name)
               (name column)))))

(defrecord Join [a b condition]
  SQLAble
  (-to-sql [_]
    (let [[s v] (-to-sql a)
          [s2 v2] (-to-sql b)
          [s3 v3] (-to-sql condition)
          known-cols (into (set (columns a)) (columns b))]
      (assert (every? known-cols (columns condition)))
      [(str s " JOIN " s2 " ON " s3)
       (into (into v v2) v3)]))
  (columns [_]
    (distinct
     (concat (columns a)
             (columns b)))))

(defrecord Project [a scolumns]
  SQLAble
  (-to-sql [_]
    (let [[s v] (-to-sql a)
          known-cols (set (columns a))]
      (assert (every? known-cols scolumns))
      [(str "SELECT " (apply str (interpose "," (map first (map -to-sql scolumns))))
            " FROM "
            s)
       v]))
  (columns [_]
    scolumns))

(defrecord Select [a condition]
  SQLAble
  (-to-sql [_]
    (let [[s v] (-to-sql a)
          known-cols (set (columns a))
          _ (assert (every? known-cols (columns condition)))
          [s1 v1] (-to-sql condition)]
      [(str s
            " WHERE "
            s1)
       (into v v1)]))
  (columns [_]
    (columns a)))

(defrecord Literal [value]
  SQLAble
  (-to-sql [_]
    ["?" [value]])
  (columns [_]
    []))

(defn join [a b condition]
  (->Join a b condition))

(defn ⨝ [a b condition]
  (join a b condition))

(defn project [a & columns]
  (->Project a columns))

(defn π [a & columns]
  (apply project a columns))

(defn select [a condition]
  (->Select a condition))

(defn σ [a condition]
  (select a condition))

(defn lit [v]
  (->Literal v))

(defn as [table new-name]
  (->As table new-name))

(def max-buffer 10)

(defonce buffer (agent {:sql [] :place 0}))

(defn to-sql [x]
  (let [[s v] (-to-sql x)]
    (send-off buffer (fn [{:keys [sql place]} query]
                       (let [new-sql (assoc sql place query)
                             new-place (mod (inc place) max-buffer)]
                         {:sql new-sql :place new-place}))
              s)
    (into [s] v)))

(defn t [name & columns]
  (->Table name (set columns)))


;; (let [ng (->Table :named_graph #{:id :graph_id :name :tag})
;;       g (->Table :graph #{:id :x :tag})
;;       gfs (->Table :graph_fragments #{:id :fragment_id :graph_id})]
;;   (-> (join g (as ng :bob) (equals :bob/graph_id :graph/id))
;;       (join (as gfs :gfs) (equals :gfs/graph_id :graph/id))
;;       (select (equals :bob/name (lit (System/currentTimeMillis))))
;;       (project :graph/id)
;;       (to-sql)))

;; ;; (def fragment (->Table #{:id :size :tag}))
;; ;; (def graph-fragments (->Table #{:id :graph_id :fragment_id}))
;; ;; (def edge (->Table #{:id :fragment_id :vid :src :dest :weight :tag}))
;; ;; (def node (->Table #{:id :fragment_id :vid :tag}))
