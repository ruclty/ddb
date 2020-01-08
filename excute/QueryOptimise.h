#ifndef _QUERY_OP_H
#define _QUERY_OP_H
#include <string>
#include "../querytree/querytree.h"
#include "../global.h"
#include "../socket/rpc_sent.cpp"
#include "../gdd/gdd.h"
#include <vector>
#include <map>
#include <algorithm>
using namespace std;

const int MAIN_SITE_ID = 1;
class query_plan{
	private:
		bool s_join = false;
		query_tree_node* root; 	
		map<int,vector<Operator>> plan; 		
	public:
		query_plan(query_tree_node*);
		~query_plan();
		void excute_query_plan(query_tree_node*,int);

		//void partition_range(Operator);
    	vector<frag_info> how_to_transfer(vector<int>);
    	vector<frag_info> how_transfer(vector<int>);
		void excute_one_operator(query_tree_node*, int);
//        GDD gdd = GDD();
		int semi_join(int, int, int);


		int load_datafile(string , vector<string> , vector<string> , string , string ,int );
		//this two function need rpc. first 3 parameters.
		int transfer_sql(string , int , int ,vector<string>);
        int transfer_table(string , int , int );
		void transfer_plan();
		void start_excute();
		int allocate(string , int , map<string,int> );
			//if JOIN,SELECTION,PROJECTION: return the result table after sourceSite excute the sql statement,which is generated by Excute.(sql is support by class Mysql)
			//if TRANSFORM: return a temp table to save the targetTable on targetSite.
			//if UNION: return result table use sql statement(insert)
			//Post order traversal, and before excute join, do semi_join; before excute transfer, do how_to_transform and partition_range(*).
};


#endif
