package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

var shard1 *sql.DB
var shard2 *sql.DB

func NewConnection(dbName string) *sql.DB {
	// Open a database connection
	dbUrl := fmt.Sprintf("root:123456@tcp(localhost:3306)/%s", dbName)
	db, err := sql.Open("mysql", dbUrl)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func init() {
	// initialization of shards
	shard1 = NewConnection("demoshardone")
	shard2 = NewConnection("demoshardtwo")
}

func getUserNameFromDB(userId int) string {
	var name string

	// if userId is less than 100, then call shard-1. For id's more than 100 , call shard-2
	if userId < 100 {
		log.Println("Shard-1 is called.")
		rows := shard1.QueryRow("SELECT name FROM user WHERE id=?", userId)
		scanerr := rows.Scan(&name)
		if scanerr != nil {
			log.Fatalf("Unable to decode user name : %v", scanerr.Error())
		}
	} else {
		log.Println("Shard-2 is called.")
		rows := shard2.QueryRow("SELECT name FROM user WHERE id=?", userId)
		scanerr := rows.Scan(&name)
		if scanerr != nil {
			log.Fatalf("Unable to decode user name : %v", scanerr.Error())
		}
	}

	return name
}

func getUserInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	user_id := vars["user_id"]
	parsedId, parseErr := strconv.ParseInt(user_id, 10, 64)
	username := getUserNameFromDB(int(parsedId))
	if parseErr != nil {
		log.Fatalf("Unable to parse user_id : %v", parseErr.Error())
	}
	// send json response
	json.NewEncoder(w).Encode(map[string]string{
		"username": username,
		"userid":   user_id,
	})
}

func main() {
	log.Println("Welcome to db sharding")
	router := mux.NewRouter()
	router.HandleFunc("/user/{user_id}", getUserInfo).Methods("GET")
	log.Println("Server is listening at port : 8000...")
	log.Fatal(http.ListenAndServe(":8000", router))
}
