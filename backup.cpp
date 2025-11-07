// Compile: g++ simple_lru_postgres_server.cpp -o simple_lru_postgres_server -std=c++11 -lpqxx -lpq
// Requires: libpqxx (PostgreSQL C++ client library)
#include "httplib.h"
#include <iostream>
#include <string>
#include <sstream>
#include <pqxx/pqxx>
using namespace std;

#define PORT_NO 5078
#define CACHE_SIZE 5   // Maximum items in cache

const string db_conn_str = "dbname=unagarsahil user=postgres password=Unagar@1248 host=localhost port=5432";


// Node for linked list (LRU cache)
struct Node {
    int key;
    string value;
    Node *next;
};

Node *head = NULL;
int cache_size = 0;

// Move a node with given key to the front (MRU)
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

// Delete last (LRU) node when cache is full
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

// Find value for a key in cache
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

// Insert a new key-value pair into cache
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

// Update value in cache
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

// Delete key from cache
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

// Print cache contents
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

// ========== PostgreSQL Functions ==========

// Read value from PostgreSQL
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
        return "FAILED TO CREATE KEY IN DB";
    }
}

// Update key in PostgreSQL
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
        return "FAILED TO UPDATE KEY IN DB";
    }
}

// Delete key in PostgreSQL
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

// ========== Main Server ==========

int main() {
    // Prepare PostgreSQL statements
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

    // ----------- CREATE -----------
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

    // ----------- READ -----------
    server.Get("/read", [](const httplib::Request &req, httplib::Response &res) {
        if (!req.has_param("key")) {
            res.set_content("Missing key parameter", "text/plain");
            return;
        }
        int key = stoi(req.get_param_value("key"));

        string val = read_cache(key); // first check cache
        if (val != "") {
            res.set_content("Cache hit: " + val, "text/plain");
            return;
        }

        val = read_db(key); // cache miss, go to DB
        if (val == "")
            res.set_content("KEY NOT FOUND", "text/plain");
        else {
            insert_cache(key, val); // store in cache
            res.set_content("DB hit: " + val, "text/plain");
        }
    });

    // ----------- UPDATE -----------
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
        update_cache(key, value); // update cache if present
        res.set_content(msg, "text/plain");
    });

    // ----------- DELETE -----------
    server.Post("/delete", [](const httplib::Request &req, httplib::Response &res) {
        if (!req.has_param("key")) {
            res.set_content("Missing key parameter", "text/plain");
            return;
        }
        int key = stoi(req.get_param_value("key"));

        string msg = delete_db(key);
        delete_cache(key); // also remove from cache
        res.set_content(msg, "text/plain");
    });

    // ----------- PRINT CACHE -----------
    server.Get("/print", [](const httplib::Request &req, httplib::Response &res) {
        res.set_content(print_cache(), "text/plain");
    });

    cout << "LRU Cache + PostgreSQL HTTP server running on port " << PORT_NO << endl;
    server.listen("0.0.0.0", PORT_NO);

    return 0;
}
