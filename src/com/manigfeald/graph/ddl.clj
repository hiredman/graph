(ns com.manigfeald.graph.ddl
  (:require [clojure.java.jdbc :as jdbc]))

(def text "varchar(1024)")

(def vid "CHAR(16) FOR BIT DATA")

(defn create-named-graph [v]
  (jdbc/create-table-ddl
   v
   [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
   [:graph_id :int]
   [:name text]
   [:tag :int]))

(defn create-graph [v]
  (jdbc/create-table-ddl
   v
   [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
   [:x :int]
   [:tag :int]))

(defn create-fragment [v]
  (jdbc/create-table-ddl
   v
   [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
   [:size :int]
   [:tag :int]))

(defn create-graph-fragments [v]
  (jdbc/create-table-ddl
   v
   [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
   [:graph_id :int]
   [:fragment_id :int]
   [:tag :int]))

(defn create-node [v]
  (jdbc/create-table-ddl
   v
   [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
   [:fragment_id :int]
   [:vid vid]
   [:tag :int]))

(defn create-edge [v]
  (jdbc/create-table-ddl
   v
   [:id :int "PRIMARY KEY" "GENERATED ALWAYS AS IDENTITY"]
   [:fragment_id :int]
   [:vid vid]
   [:src vid]
   [:dest vid]
   [:weight :int]
   [:tag :int]))

(defn create-attribute [v k]
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
   [:tag :int]))
