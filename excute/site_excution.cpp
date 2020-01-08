#include <queue>
#include <iostream>
using namespace std;

#define MAIN_SITE_ID = 1;

site_excution::site_excution(int site_id){
	this->site_id = site_id;
	this->mysql = MySql(site_id);
	this->table_queue = mysql.get_table_names();
}

site_excution::~site_excution(){}

vector<Operator> site_excution::check_plan(){
	vector<Operator> results;
	if(this->sql_queue.size == 0){
		results.push_back("finish!");
		return results;
	}

	for(int i=0; i< this->sql_queue.size(); i++){
		// Operator now_sql;
		// now_sql.content = this->sql_queue;
		for(int j=0; j< i.table_names.size(); j++){
			bool no_in = false;
			for(int k = 0; k < this->table_queue.size;k++){// find table from table_queue
				if(table_queue[k] == i.table_names[j]){
					no_in = true;
					break
				}
			}
			if(no_in == false){// if not_find table ,this sql can't excute;
				can_excute = false;
				break;
			}
			
		}
		
	}
	return results;

}


vector<Operator> site_excution::recieve_and_check(int frag_id,string table_content, string origin_table_name){
	//string table_name = this->mysql->gdd->get_frag_info(frag_id).table_name;
	this->mysql->create_received_table(table_content,frag_id,origin_table_name,false);
	this->table_queue.push_back(recieved_table_name);
	vector<Operator> results;
	results = this->check_plan();
	return results;
}

void site_excution::recieve_result_table(int frag_id,string table_content, string origin_table_name){
	//string table_name = this->mysql->gdd->get_frag_info(frag_id).table_name;
	this->mysql->create_received_table(table_content,frag_id,origin_table_name,true);
	frag_info source_table_info = gdd.get_frag_info(origin_frag_id);
    vector<string> table_attr_name = source_table_info.attr_names;
    for(int i=0; i<table_attr_name.size();i++){
    	string temp = "";
    	temp += table_attr_name[i];
    	temp += ' ';
    	if(i != table_attr_name.size()-1)
    		temp += ',';
    	cout << temp << endl;

    	cout << table_content << endl;
    }
}

vector<Operator> site_excution::recieve_plan(vector<Operator> plan){
	vector<Operator> results;
	this->sql_queue = plan;
	results = this->check_plan();
	return results;
}

void site_excution::excute_results(vector<Operator> to_Operator){
	for(int i=0; i < to_Operator.size(); i++){
		string sql = Operator.content;
		vector<result> results;
		result res = result();
		if(Operator.ope == ESS){
			string temp_name = Operator.result_frag_id;
			res.result_frag_id = this->mysql.excute_select_sql(sql, result_frag_id);
			table_queue.push_back(temp_name);
			
			if(Operator.is_end == 1):{
				string table_name = gdd.get_frag_info.table_name(result_frag_id);
				res.table = mysql.select_all_table(table_name);
				bool sent = SendResultTable(Operator.result_frag_id, res.table,table_name, MAINSITE, this->site_id);
			}
		}
		if(Operator.ope == SAT){
			res.result_frag_id = Operator.result_frag_id;
			res.table = this->mysql.select_all_table(sql);
			bool sent = SendTable(Operator.result_frag_id ,res.table, sql,Operator.target_site_id,this->site_id);

			if(Operator.is_end == 1):, 
				bool sent = SendResultTable(Operator.result_frag_id,res.table, MAINSITE, this->site_id);
		}
		for(auto it = this->sql_queue.begin(); it != this->sql_queue.end();){
			if(*it == to_Operator[i]){
				it = this->sql_queue.erase(it);
				break;
			}
		}
		results.push_back(res);
		string table_name = this->mysql.gdd.get_frag_info(res.result_frag_id).table_name;
		this->table_queue.push_back(table_name);
	}
}