package main

import (
	"database/sql"
	"encoding/json"
	"strconv"

	"fmt"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"

	_ "github.com/lib/pq"
)

var db *sql.DB

const (
	host     = "atom-bank-challenge-database.clh6qn7s7rbm.eu-west-1.rds.amazonaws.com"
	port     = 5432
	user     = "postgres"
	password = "gtR0tC5%PX99"
	dbname   = "atom"
)

type BankUser struct {
	UserId        string `json:"userId"`
	Forename      string `json:"forename"`
	Surname       string `json:"surname"`
	AccountNumber string `json:"accountNumber"`
	SortCode      string `json:"sortCode"`
}

type Transaction struct {
	TransactionID    string  `json:"transactionId"`
	Description      string  `json:"transactionDescription"`
	Amount           float64 `json:transactionAmount`
	PayeeID          string  `json:payeeID`
	RecipiedID       string  `json:recipientID`
	GroupDescription string  `json:groupDescription`
	TransactionDate  string  `json:transactionDate`
}

type Balance struct {
	BalanceTotal float64 `json:"balance"`
}

type NewTransaction struct {
	Description string `json: "description"`
	Amount      string `json: "amount"`
	Payee       string `json: "payee"`
	Recipient   string `json: "recipient"`
	GroupID     string `json: "groupId"`
}

func main() {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	var err error
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		print("VERY BIG PANIC!\n")
		log.Panic(err)
	}
	router := httprouter.New()
	router.GET("/bankuser/:id", getUser)
	router.GET("/transactions/in/:userid", getReceivingTrasactions)
	router.GET("/transactions/out/:userid", getPayingTrasactions)
	router.GET("/balance/:userid", getBalanceUser)
	router.POST("/transaction", addTransaction)

	log.Fatal(http.ListenAndServe(":5000", router))
}

func addTransaction(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var err error
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	t := new(NewTransaction)

	err = d.Decode(&t)
	if err != nil {
		// bad JSON or unrecognized json field
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	amount, err := strconv.ParseFloat(t.Amount, 64)
	if err != nil {
		panic(err)
	}
	println(amount)
	queryString := "INSERT INTO transactions (description, amount, payee, recipient, group_id) VALUES ('" + t.Description + "'," + t.Amount + ",'" + t.Payee + "','" + t.Recipient + "','" + t.GroupID + "');"
	_, err = db.Query(queryString)
	if err != nil {
		panic(err)
	}
}

func getBalanceUser(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var err error
	theUserId := ps.ByName("userid")
	queryStringOut := "SELECT SUM(amount) from transactions where payee='" + theUserId + "';"
	var outgoing float64
	row := db.QueryRow(queryStringOut)
	err = row.Scan(&outgoing)
	if err != nil {
		print("query error")
	}

	queryStringIn := "SELECT SUM(amount) from transactions where recipient='" + theUserId + "';"
	var incoming float64
	out := db.QueryRow(queryStringIn)
	err = out.Scan(&incoming)
	if err != nil {
		print("Scan error")
	}

	bal := Balance{incoming - outgoing}
	js, err := json.Marshal(bal)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func getPayingTrasactions(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	theUserId := ps.ByName("userid")
	print(theUserId)
	rows, err := db.Query("SELECT transaction_id, description, amount, payee, recipient, group_description, datetime FROM (transactions LEFT JOIN transaction_groups ON transactions.group_id = transaction_groups.group_id) where transactions.payee = '" + theUserId + "';")
	if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}
	defer rows.Close()
	transactions := make([]*Transaction, 0)
	for rows.Next() {
		tran := new(Transaction)
		err := rows.Scan(&tran.TransactionID, &tran.Description, &tran.Amount, &tran.PayeeID, &tran.RecipiedID, &tran.GroupDescription, &tran.TransactionDate)
		if err != nil {
			print("big error")
		}
		log.Println(tran.Amount)
		transactions = append(transactions, tran)
	}
	js, err := json.Marshal(transactions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func getReceivingTrasactions(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	theUserId := ps.ByName("userid")
	print(theUserId)
	rows, err := db.Query("SELECT transaction_id, description, amount, payee, recipient, group_description, datetime FROM (transactions LEFT JOIN transaction_groups ON transactions.group_id = transaction_groups.group_id) where transactions.recipient = '" + theUserId + "';")
	if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}
	defer rows.Close()
	transactions := make([]*Transaction, 0)
	for rows.Next() {
		tran := new(Transaction)
		err := rows.Scan(&tran.TransactionID, &tran.Description, &tran.Amount, &tran.PayeeID, &tran.RecipiedID, &tran.GroupDescription, &tran.TransactionDate)
		if err != nil {
			print("big error")
		}
		transactions = append(transactions, tran)
	}
	js, err := json.Marshal(transactions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func getUser(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	theUserId := ps.ByName("id")
	if r.Method != "GET" {
		http.Error(w, http.StatusText(405), 405)
		return
	}

	rows, err := db.Query("SELECT * FROM bank_user where user_id = '" + theUserId + "';")
	if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}
	defer rows.Close()

	usrs := make([]*BankUser, 0)
	for rows.Next() {
		usr := new(BankUser)
		err := rows.Scan(&usr.UserId, &usr.Forename, &usr.Surname, &usr.AccountNumber, &usr.SortCode)
		if err != nil {
			print("big error")
		}
		usrs = append(usrs, usr)
	}
	js, err := json.Marshal(usrs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func getJSON(sqlString string) (string, error) {
	rows, err := db.Query(sqlString)
	if err != nil {
		return "", err
	}

	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	jsonData, err := json.Marshal(tableData)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}
