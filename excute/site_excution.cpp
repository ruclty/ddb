#include <queue>
#include <iostream>
#include "../socket/rpc_sent.cpp"
#include "site_excution.h"
using namespace std;

#define MAIN_SITE_ID 5

site_excution::site_excution(int site_id){
	this->site_id = site_id;
	string ddb_name = "site"+to_string(site_id);
	//cout << ddb_name << endl;
	this->mysql = MySql(ddb_name,site_id);
	this->table_queue = this->mysql.get_table_names();
	for(int i=0; i< table_queue.size(); i++)
		cout << this->table_queue[i] << endl;
}

site_excution::~site_excution(){}

vector<Operator> site_excution::check_plan(){
	vector<Operator> results;
	if(this->sql_queue.size() == 0){
		
		//results.push_back("finish!");
		return results;
	}

	for(int i=0; i< this->sql_queue.size(); i++){
		// Operator now_sql;
		// now_sql.content = this->sql_queue;
		cout << "find is some table can exc" << endl;
		bool can_excute = true;
		this->sql_queue[i].id = i;
		cout << "plan i content:" << sql_queue[i].content << endl;
		cout << to_string(sql_queue[i].table_names.size()) << endl;
		//cout <<"sql:is_end:" + to_string(sql_queue[i].is_end) << endl;
		if(sql_queue[i].table_names.size() != 0){
			for(int j=0; j< sql_queue[i].table_names.size(); j++){
				bool no_in = false;
				cout << "sql:talbe:"+sql_queue[i].table_names[j] << endl;
	
				if(can_excute == false)
					break;
				for(int k = 0; k < this->table_queue.size();k++){// find table from table_queue
		//			cout << "table_queue_table:" + table_queue[k] << endl;		
						if(table_queue[k] == sql_queue[i].table_names[j]){
							no_in = true;
							cout <<"can excute:" + sql_queue[i].table_names[j] << endl;
							//break; 
							}
						
					
				}
				if(no_in == false){// if not_find table ,this sql can't excute;
					can_excute = false;
					break;
				}
				
			}
			if(can_excute == true){
				cout << "can excute!!!!!!!!!!!"+ sql_queue[i].content << endl;
				results.push_back(sql_queue[i]);
			}
				
		}
		else{
			for(int k = 0; k < this->table_queue.size();k++){
				if(table_queue[k] == sql_queue[i].content){
					can_excute = true;
					cout <<"can excute:" + sql_queue[i].content << endl;
					results.push_back(sql_queue[i]);
					break;
				}
			}
		
		}
			
			
	}
	for(int i=0;i<results.size();i++){
		for(auto it = this->sql_queue.begin(); it != this->sql_queue.end();){
			//cout << *it << endl;
			//cout << to_Operator[i] << endl;
			if(it->id == results[i].id){
				it = this->sql_queue.erase(it);
				cout << "ereased!!!!" << endl;
				break;
			}
		}
	}
	cout <<"res_size:"+ to_string(results.size()) << endl;
//	for(int i=0;i<results.size();i++)
//		cout << "result op:" + to_string(results[i].id) << endl;
	return results;

}


vector<Operator> site_excution::recieve_and_check(int frag_id,string table_content, string origin_table_name){
	//string table_name = this->mysql.gdd->get_frag_info(frag_id).table_name;
	string received_table_name = get_frag_name(frag_id);
	this->mysql.create_received_table(table_content,frag_id,origin_table_name,false);
	this->table_queue.push_back(received_table_name);
	vector<Operator> results;
	results = this->check_plan();
	return results;
}

void site_excution::recieve_result_table(int frag_id,string table_content, string origin_table_name){
	//string table_name = this->mysql.gdd->get_frag_info(frag_id).table_name;
	this->mysql.create_received_table(table_content,frag_id,origin_table_name,true);
	int origin_frag_id = from_name_find_frag_id(origin_table_name);
	frag_info source_table_info = get_frag_info(origin_frag_id);
    map<int,attr_info> table_attributes = source_table_info.attr_infos;
    string temp = "";
    for(int i=0; i<table_attributes.size();i++){
    		attr_info atr = table_attributes[i+1];
    		
    		temp += atr.attr_name;
    		if(i != table_attributes.size()-1){
    			temp += ',';
    			}
    }
    cout << temp  << endl;
    cout << table_content << endl;
    for(int i=1;i<=4;i++)
    		SendDeleteTemp(i,5);
}

vector<Operator> site_excution::recieve_plan(vector<Operator> plan){
	vector<Operator> results;
	this->sql_queue = plan;
	results = this->check_plan();
	return results;
}

void site_excution::excute_results(vector<Operator> to_Operator){
	for(int i=0; i < to_Operator.size(); i++){
		string sql = to_Operator[i].content;
		vector<result> results;
		result res = result();
		if(to_Operator[i].ope == ESS){
			string temp_name = get_frag_name(to_Operator[i].result_frag_id);
			
			res.result_frag_id = this->mysql.excute_select_sql(sql, to_Operator[i].result_frag_id);
			table_queue.push_back(temp_name);
			//this->check_plan();
			if(to_Operator[i].is_end == 1){
				string table_name = get_frag_name(to_Operator[i].result_frag_id);
				res.table_content = mysql.select_all_table(table_name);
				SendResultTable(to_Operator[i].result_frag_id, res.table_content,table_name, MAIN_SITE_ID, this->site_id);
			}
		}
		if(to_Operator[i].ope == SAT){
			res.result_frag_id = to_Operator[i].result_frag_id;
			res.table_content = this->mysql.select_all_table(sql);
			SendTable(to_Operator[i].result_frag_id ,res.table_content, sql,to_Operator[i].target_site_id,this->site_id);

			if(to_Operator[i].is_end == 1) 
				SendResultTable(to_Operator[i].result_frag_id,res.table_content, sql,5, this->site_id);
		}

		results.push_back(res);
		string table_name = get_frag_name(res.result_frag_id);
		this->table_queue.push_back(table_name);
	}
	vector<Operator> next_Operator = this->check_plan();
	if(next_Operator.size()>0)
		excute_results(next_Operator);
}
