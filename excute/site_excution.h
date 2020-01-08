#include <queue>
#include <iostream>
//#include "../global.h"
#include "../rpc/rpc_sent.h"
#include "./local_sql_excution.h"
using namespace std;


struct result{
	int result_frag_id;
	string table_content;// true:sql, false:table content
};

class site_excution{
	public:
		int site_id;

		site_excution(int);
		~site_excution();
		vector<Operator> sql_queue;
		vector<string> table_queue;
	
		MySql mysql = MySql(" ",site_id);

		vector<Operator> check_plan();
		vector<Operator> recieve_plan(vector<Operator>);
		vector<Operator> recieve_and_check(int ,string,string);
		void recieve_result_table(int frag_id,string table_content, string origin_table_name);
		void excute_results(vector<Operator> );
		//void get_queue(vector<Operator>);
		//vector<string> get_table_names(string sql);
};
