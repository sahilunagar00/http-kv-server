#include "httplib.h"
#include <iostream>
#include <string>
#include <sstream>
#include <pqxx/pqxx>
using namespace std;

#define PORT_NO 5078
#define CACHE_SIZE 5   // maximum no of nodes in cache

const string db_conn_str = "dbname=unagarsahil user=postgres password=Unagar@1248 host=localhost port=5432";

struct Node {
    int key;
    string value;
    Node *next;
};

Node *head = NULL;
int cache_size = 0;

// move a node with given key in front
void move_to_front(int key) {
    if (head == NULL || head->key == key) return;

    Node *prev = NULL, *cur = head;
    while (cur != NULL && cur->key != key) {
        prev = cur;
        cur = cur->next;
    }
    if (cur == NULL) return; // not found

    prev->next = cur->next;
    cur->next = head;
    head = cur;
}

// delete last node when cache is full
void delete_last() {
    if (head == NULL) return;
    if (head->next == NULL) {
        delete head;
        head = NULL;
        cache_size--;
        return;
    }

    Node *cur = head;
    while (cur->next->next != NULL) cur = cur->next;
    delete cur->next;
    cur->next = NULL;
    cache_size--;
}

// find value for given key in cache
string read_cache(int key) {
    Node *temp = head;
    while (temp != NULL) {
        if (temp->key == key) {
            string val = temp->value;
            move_to_front(key);
            return val;
        }
        temp = temp->next;
    }
    return "";
}

// insert new key-value pair into cache
void insert_cache(int key, string value) {
    Node *newNode = new Node;
    newNode->key = key;
    newNode->value = value;
    newNode->next = head;
    head = newNode;
    cache_size++;
    if (cache_size > CACHE_SIZE)
        delete_last();
}

// update value in cache
bool update_cache(int key, string value) {
    Node *temp = head;
    while (temp != NULL) {
        if (temp->key == key) {
            temp->value = value;
            move_to_front(key);
            return true;
        }
        temp = temp->next;
    }
    return false;
}

// delete key from cache
bool delete_cache(int key) {
    if (head == NULL) return false;
    if (head->key == key) {
        Node *todel = head;
        head = head->next;
        delete todel;
        cache_size--;
        return true;
    }
    Node *cur = head;
    while (cur->next != NULL && cur->next->key != key)
        cur = cur->next;
    if (cur->next == NULL) return false;
    Node *todel = cur->next;
    cur->next = cur->next->next;
    delete todel;
    cache_size--;
    return true;
}

// print full content of cache
string print_cache() {
    Node *temp = head;
    stringstream ss;
    ss << "Cache (Most Recent → Least Recent):\n";
    while (temp != NULL) {
        ss << temp->key << " -> " << temp->value << "\n";
        temp = temp->next;
    }
    return ss.str();
}

//postgreSQL functions

// Read value from postgreSQL
string read_db(int key) {
    try {
        pqxx::connection C(db_conn_str);
        C.prepare("select_val", "SELECT value FROM kv_store WHERE key=$1"); // <-- ADD THIS LINE
        pqxx::work W(C);
        pqxx::result R = W.exec_prepared("select_val", key);

        if (R.size() == 0)
            return "";

        string val = R[0][0].as<string>();
        W.commit();
        return val;
    } catch (const std::exception &e) {
        cerr << "Database error: " << e.what() << endl;
        return "";
    }
}

// Create key in PostgreSQL
string create_db(int key, string value) {
    try {
        pqxx::connection C(db_conn_str);
        C.prepare("insert_key", "INSERT INTO kv_store (key,value) VALUES($1,$2)"); // <-- ADD THIS LINE
        pqxx::work W(C);
        W.exec_prepared("insert_key", key, value);
        W.commit();
        return "KEY CREATED SUCCESSFULLY IN DB";
    } catch (const std::exception &e) {
        cerr << "Database error: " << e.what() << endl;
        return "FAILED TO CREATE KEY INSIDE DATABASE";
    }
}

// update key in postgreSQL
string update_db(int key, string value) {
    try {
        pqxx::connection C(db_conn_str);
        C.prepare("update_key", "UPDATE kv_store SET value=$1 WHERE key=$2"); // <-- ADD THIS LINE
        pqxx::work W(C);
        pqxx::result R = W.exec_prepared("update_key", value, key);
        W.commit();
        if (R.affected_rows() > 0)
            return "KEY UPDATED SUCCESSFULLY IN DB";
        return "KEY NOT FOUND IN DB";
    } catch (const std::exception &e) {
        cerr << "Database error: " << e.what() << endl;
        return "FAILED TO UPDATE KEY INSIDE DATABASE";
    }
}

// delete key in postgreSQL
string delete_db(int key) {
    try {
        pqxx::connection C(db_conn_str);
        C.prepare("delete_key", "DELETE FROM kv_store WHERE key=$1"); // <-- ADD THIS LINE
        pqxx::work W(C);
        pqxx::result R = W.exec_prepared("delete_key", key);
        W.commit();
        if (R.affected_rows() > 0)
            return "KEY DELETED FROM DB";
        return "KEY NOT FOUND IN DB";
    } catch (const std::exception &e) {
        cerr << "Database error: " << e.what() << endl;
        return "FAILED TO DELETE KEY IN DB";
    }
}

// Main Server

int main() {
    // Prepare postgreSQL statements
    try {
        pqxx::connection C(db_conn_str);
        pqxx::work W(C);
        W.exec("CREATE TABLE IF NOT EXISTS kv_store (key INT PRIMARY KEY, value TEXT)");
        W.commit();
        // All C.prepare() lines removed from here
    } catch (const std::exception &e) 
    {
        cerr << "Database initialization error: " << e.what() << endl;
        return 1;
    }


    httplib::Server server;

    // create
    server.Post("/create", [](const httplib::Request &req, httplib::Response &res) {
        if (!req.has_param("key")) {
            res.set_content("Missing key parameter", "text/plain");
            return;
        }
        int key = stoi(req.get_param_value("key"));
        string value = req.body;
        if (value.empty()) {
            res.set_content("Missing value", "text/plain");
            return;
        }

        string msg = create_db(key, value); // insert in DB
        insert_cache(key, value);           // also cache it
        res.set_content(msg, "text/plain");
    });

    // read endpoint
    server.Get("/read", [](const httplib::Request &req, httplib::Response &res) {
        if (!req.has_param("key")) {
            res.set_content("Missing key parameter", "text/plain");
            return;
        }
        int key = stoi(req.get_param_value("key"));

        cout << "Key to read : " << key << '\n';

        string val = read_cache(key); // first we try to check in cache
        if (val != "") {
            res.set_content("Cache hit: " + val, "text/plain");
            return;
        }

        val = read_db(key); // cache miss, search  in database
        if (val == "")
            res.set_content("KEY NOT FOUND", "text/plain");
        else {
            // data is in databse but not in cache so we retrive from database and push into cache
            insert_cache(key, val); 
            res.set_content("DB hit: " + val, "text/plain");
        }
    });

    // update endpoint
    server.Post("/update", [](const httplib::Request &req, httplib::Response &res) {
        if (!req.has_param("key")) {
            res.set_content("Missing key parameter", "text/plain");
            return;
        }
        int key = stoi(req.get_param_value("key"));
        string value = req.body;
        if (value.empty()) {
            res.set_content("Missing value", "text/plain");
            return;
        }

        string msg = update_db(key, value);
        //key may also present in cache so need to update cache also
        update_cache(key, value); 
        res.set_content(msg, "text/plain");
    });

    //delete endpoint
    server.Delete("/delete", [](const httplib::Request &req, httplib::Response &res) {
        cout << "in delete func : " << endl;
        if (!req.has_param("key")) {
            res.set_content("Missing key parameter", "text/plain");
            return;
        }


        int key = stoi(req.get_param_value("key"));

        cout << "Key to delete : " << key << endl;

        string msg = delete_db(key);
        //key also present in cache . so we need to remove key from cache
        delete_cache(key);
        res.set_content(msg, "text/plain");
    });

    //print entire cache
    server.Get("/print", [](const httplib::Request &req, httplib::Response &res) {
        res.set_content(print_cache(), "text/plain");
    });

    cout << "LRU Cache + PostgreSQL HTTP server running on port " << PORT_NO << endl;
    server.listen("0.0.0.0", PORT_NO);

    return 0;
}
