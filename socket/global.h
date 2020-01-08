#ifndef _GLOBAL_H
#define _GLOBAL_H

#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include <tuple>
#include <queue>
#include <map>
#include <regex>
using namespace std;

#define BUFFER_SIZE 1024
#define SERVER_PORT 6666

enum OPERATORTYPE {
	ESS, SAT, CRT, RATT, LD, REC
};    

struct Operator{
	string content;
	OPERATORTYPE ope;
	int result_frag_id;	
	int target_site_id;
	int is_end ;
	vector<string> table_names;

	// select * from table where...-> ESS;
	// table_name, target_frag_id -> SAT;

};

enum NODETYPE {
	SELECT, PROJECT, JOIN, UNION, TABLE, FRAGMENT
};    // 查询树中的节点类型

enum RELATION {
	EQ, NE, G, GE, L, LE
};    // 谓词重的关系类型  =, !=, >, >=, <, <=

enum PREDTYPE {
	INT, CHAR, TAB
};    // 谓词类型

#endif
