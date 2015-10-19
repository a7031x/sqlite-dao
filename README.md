
<h1>C++ SQLite DAO</h1>
 
Most of software that use database wouldn’t use the basic SQL query directly. The main reasons are SQL query is type unsafe and the query syntax is tedious.

SQLite is an open source, light weight database. It’s easy to port to desktop apps, mobile phones or other devices. This article a convenient data access object which is based on SQLite C++ interface. Let’s start with some examples.

Creating or opening a database

    auto d = make_shared<dao>();
    d->open("test.db");

if using the basic query,

    sqlite3 *db;
    sqlite3_open("test.db", &db);

Creating a table,

    table_adapter adapter(d, "test_table");
    adapter.create_table(
        "id integer primary key,"
        "name text,"
        "age int,"
        "image blob"
        );

if using the basic query,
 
    string createQuery =
        "create table if not exists test_table ("
        "id integer primary key,"
        "name text,"
        "age int,"
        "image blob);";

    sqlite3_stmt *createStmt;
    sqlite3_prepare(db,
        createQuery.c_str(),
        createQuery.size(),
        &createStmt,
        nullptr);

    if (sqlite3_step(createStmt) != SQLITE_DONE)
        cout << "Didn’t Create Table!" << endl;
    else
        sqlite3_finalize(createStmt);

Inserting records,

        adapter += Values("id", 1)
                   ("name", "Tom")
                   ("age", 20)
                   ("image", vector<char>{ 1, 2, 3 });

The sequence of parenthesis seems a little bit strange. Actually the parenthesis is an overriding operator which adds a key-value pair to the values and returns the reference of itself.

if using the basic query,

    sqlite3_stmt *stmt = nullptr;
    int rc = sqlite3_prepare(db,
        "INSERT INTO ONE(ID, NAME, AGE, IMAGE)"
        " VALUES(1, ‘Tom’, 20, ?)",
        -1, &stmt, nullptr);

    if (rc != SQLITE_OK) {
        cerr << "prepare failed: " << sqlite3_errmsg(db) << endl;
    }
    else {
        vector<char> image = { 1, 2, 3 };
        rc = sqlite3_bind_blob(stmt, 1, image.data(), 3, SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            cerr << "bind failed: " << sqlite3_errmsg(db) << endl;
        }
        else {
            rc = sqlite3_step(stmt);
            if (rc != SQLITE_DONE)
                cerr << "execution failed: " << sqlite3_errmsg(db) << endl;
        }
        sqlite3_finalize(stmt);
    }

Querying records,


    table t;
    adapter >> t;

string name = t[0]["name"];

Querying records with "where" clause,

    adapter["id=1"] >> t;

Deleting records,

    adapter -= Values("id", 1); //delete record with id == 1
    adapter -= Values;         //delete all records

Guess I don’t need to go for the basic query implementation. The gimmick to make it so simple is the usage of operator overriding and the boost::variant library. There are other handy operators for update, upsert, querying specified columns, etc.
