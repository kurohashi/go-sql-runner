package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// ---- CONFIG ----
const (
	addr = ":3000"
	dsn  = "root:password@tcp(localhost:3306)/test_db"
)

var db *sql.DB

// ---- REQUEST / RESPONSE ----

type QueryRequest struct {
	SQL string `json:"sql"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// ---- HANDLER ----

func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "Invalid JSON body",
		})
		return
	}

	sqlQuery := strings.TrimSpace(req.SQL)
	if sqlQuery == "" {
		respondJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "SQL query is required",
		})
		return
	}

	queryType := strings.ToUpper(strings.Fields(sqlQuery)[0])

	switch queryType {

	case "SELECT":
		rows, err := db.Query(sqlQuery)
		if err != nil {
			respondErr(w, err)
			return
		}
		defer rows.Close()

		columns, _ := rows.Columns()
		results := []map[string]interface{}{}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))

			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				respondErr(w, err)
				return
			}

			row := map[string]interface{}{}
			for i, col := range columns {
				if b, ok := values[i].([]byte); ok {
					row[col] = string(b)
				} else {
					row[col] = values[i]
				}
			}
			results = append(results, row)
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"type":  "SELECT",
			"rows":  results,
			"count": len(results),
		})

	case "INSERT", "UPDATE", "DELETE":
		res, err := db.Exec(sqlQuery)
		if err != nil {
			respondErr(w, err)
			return
		}

		affected, _ := res.RowsAffected()
		insertID, _ := res.LastInsertId()

		response := map[string]interface{}{
			"type":         queryType,
			"affectedRows": affected,
		}

		if queryType == "INSERT" {
			response["insertId"] = insertID
		}

		respondJSON(w, http.StatusOK, response)

	default:
		// CREATE / ALTER / DROP / TRUNCATE / etc.
		if _, err := db.Exec(sqlQuery); err != nil {
			respondErr(w, err)
			return
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"type":   queryType,
			"status": "executed",
		})
	}
}

// ---- HELPERS ----

func respondErr(w http.ResponseWriter, err error) {
	log.Println(err)
	respondJSON(w, http.StatusInternalServerError, ErrorResponse{
		Error:   "Query execution failed",
		Message: err.Error(),
	})
}

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// ---- MAIN ----

func main() {
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	if err = db.Ping(); err != nil {
		log.Fatal("DB connection failed:", err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	http.HandleFunc("/query", queryHandler)

	log.Println("🚀 Server running on", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
