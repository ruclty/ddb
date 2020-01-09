#ifndef QUERY_TREE_H
#define QUERY_TREE_H

#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include <tuple>
#include <queue>
#include <map>
#include "../global.h"
#include "../gdd/gdd.h"
using namespace std;

struct query_tree_node{
	NODETYPE node_type;
	
	vector<string> table_names;
	vector<string> attr_names;
	int frag_id;
	vector<int> frag_ids;
	
	PREDTYPE pred_type;
	vector<predicateV> predv;
	vector<predicateS> preds;
	vector<predicateT> predt;
	
	vector<query_tree_node*> child;
	query_tree_node* parent;
	string get_str(void);
	void remove_null_child(void);
	int get_child_index(query_tree_node*);
	query_tree_node* copy();
};

class query_tree
{
private:
	query_tree_node* root;
	string sql;
	string sel_items="";
	string from_items="";
	string where_items="";
	vector<predicateV> predv;
	vector<predicateS> preds;
	vector<predicateT> predt;
	vector<string> table_names;
	vector<string> projects;
	vector<query_tree_node*> join_nodes;
	vector<query_tree_node*> sel_nodes;
	vector<query_tree_node*> frag_nodes;
	map<string, query_tree_node*> leaf_table;
	map<int, query_tree_node*> leaf_frag;

public:
	query_tree(string&);
	~query_tree(); 

	void split_sql(void);
	void parser_sql(void);
	vector<string> parser_regex(string, string);
	void extract_pred(string, string, RELATION, PREDTYPE);
	void generate_tree(void);

	void optimize(query_tree_node*);
	
	void optimize(query_tree_node*, query_tree_node*);
	void frag_table();
    void frag_table_node(query_tree_node*);
	void push_select(void);
	void prune_join_node(query_tree_node*);
	void prune_join(void);
    string get_sql(void);	
	// print tree
	void print_tree(void);

    query_tree_node* get_root(void);
};

#endif
