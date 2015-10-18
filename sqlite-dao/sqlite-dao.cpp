// sqlite-dao.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "sqlite.hpp"
using namespace sqlite_hsd;

int main()
{

	auto d = make_shared<dao>();
	d->open("test.db");
	table_adapter adapter(d, "test_table");

	adapter.create_table(
		"id integer primary key,"
		"name text,"
		"age int,"
		"image blob"
		);

	adapter += Values("id", 1)
					("name", "Tom")
					("age", 20)
					("image", vector<char>{ 1, 2, 3 });

	adapter += Values("id", 2)("name", "Dick")("age", 21);

	table t;
	adapter >> t;
	adapter["id=1"] >> t;

	string name = t[0]["name"];

	adapter -= Values("id", 1);	//delete record with id == 1
	adapter -= Values;			//delete all records


    return 0;
}

