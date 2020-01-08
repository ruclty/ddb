#ifndef _LOCAL_SQL_H
#define _LOCAL_SQL_H


#include <string>
#include <vector>
#include <iostream>
#include <jdbc/mysql_connection.h>
#include <jdbc/cppconn/driver.h>
#include <jdbc/cppconn/exception.h>
#include <jdbc/cppconn/resultset.h>
#include <jdbc/cppconn/statement.h>
#include <jdbc/cppconn/prepared_statement.h>
#include <jdbc/mysql_driver.h>
#include "../gdd/gdd.h"
#include "../global.h"
//using namespace std;

#define DDBDEMO_LOCAL_SQL_H

using std::string;
using std::cout;
using std::endl;
using sql::mysql::MySQL_Driver;
using sql::Connection;
using sql::Statement;
using std::string;
using sql::ResultSet;
using std::ofstream;
using std::vector;


class MySql{
	private:
		string host = "tcp://127.0.0.1:3306";
		string user_name = "root";
		string passwd = "ddb09890";
		string db_name;
		int port = 3306;

		int site_id;
	public:
		GDD gdd = GDD();
		MySql(string, int);
		~MySql();
		vector<string> split(const string&, const string&);
		//this 3 functions
		//transfer_sql
		int excute_select_sql(string ,int);
		//transfer_table
		string select_all_table(string );
		void create_received_table(string ,int, string, bool);
		vector<string> get_table_names();
		int create_allocated_table(string,int,string);
		void release_all_temp_table();
		int load_datafile(string,vector<string>,vector<string> ,string,string);
		frag_info load_new_frag_info(int ,string, vector<string>, vector<attr_info> ,int ,int );


};


#endif
