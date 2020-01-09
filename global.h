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

#define SERVER_PORT 6666

using namespace std;
string mapIdtoIp(int id,int this_site)
{
	std::map<int ,string> maplive;
    if(this_site == 1){
		maplive[1]="127.0.0.1";//map中最简单最常用的插入添加！
    		maplive[4]="10.77.70.128";
    		maplive[2]="10.77.70.127";
    		maplive[3]="10.77.70.128";
    }
    if(this_site == 2)
    {
    		maplive[1]="10.77.70.126";//map中最简单最常用的插入添加！
    		maplive[4]="10.77.70.128";
    		maplive[2]="127.0.0.1";
    		maplive[3]="10.77.70.128";
    	}
   
    if(this_site == 3 | this_site == 4)
    {
    		maplive[1]="10.77.70.126";//map中最简单最常用的插入添加！
    		maplive[4]="127.0.0.1";
    		maplive[2]="10.77.70.127";
    		maplive[3]="127.0.0.1";
    	}

    std::map<int ,string >::iterator l_it;;
    l_it=maplive.find(id);
    if(l_it==maplive.end())
        cout<<"we do not find 112"<<endl;
    return l_it->second;
}

enum OPERATORTYPE {
	ESS, SAT, CRT, RATT, LD, REC
};    

struct Operator{
	int id;
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

string ntype_to_token(NODETYPE ntype){
    switch (ntype){
        case SELECT: return "select"; case PROJECT: return "project";
        case JOIN: return "join"; case UNION: return "union";
        case TABLE: return "table"; case FRAGMENT: return "fragment";
    }
}     

string ptype_to_token(PREDTYPE ntype){
    switch (ntype){
        case INT: return "int"; 
        case CHAR: return "char";
        case TAB: return "table"; 
    }
}     

string rela_to_token(RELATION rel){
    switch (rel){
        case EQ: return "="; case L: return "<"; case G: return ">";
        case LE: return "<="; case GE: return ">="; case NE: return "!=";
    }
}

RELATION token_to_rela(string token){
    if(token == "=")
        return EQ;
    if(token == "<")
        return L;
    if(token == ">")
        return G;
    if(token == "<=")
        return LE;
    if(token == ">=")
        return GE;
    if(token == "!=")
        return NE;
}

struct predicateV{
	string table_name;
	string attr_name;
	RELATION rel;
	int value;
	string get_str(){
		return table_name+"."+attr_name+rela_to_token(rel)+to_string(value);
	}
};     // INT 型谓词 e.g Table.attribute = 12

struct predicateS{
	string table_name;
	string attr_name;
	RELATION rel;
	string value;
	string get_str(){
		return table_name+"."+attr_name+rela_to_token(rel)+value;
	}
};      // CHAR 型谓词  e.g Table.attribute = 'string'

struct predicateT{
	string left_table;
	string left_attr;
	RELATION rel;
	string right_attr;
	string right_table;
	string get_str(){
		return left_table+"."+left_attr+rela_to_token(rel)+right_table+"."+right_attr;
	}
};     // TAB 型谓词 e.g Table1.attribute = Table2.attribute

vector<string> split(string str, string delim) {
    regex re{ delim };
    return vector<std::string> {
        sregex_token_iterator(str.begin(), str.end(), re, -1),
            sregex_token_iterator()
    };
}     // 分词

int c_in_str(string str, const char c){
    int count = 0;
    for(int i = 0;i < str.size();i ++)
        if(str[i] == c)
            count ++;
    return count;
} // 统计字符 c 在字符串 str 中出现的次数

void trim(string &s)
{
    int index = 0;
    if(!s.empty())
        while( (index = s.find(' ',index)) != string::npos)
            s.erase(index,1);
}  //去掉字符串 s 中包含的空格

bool conflict_pred(predicateV pred1, predicateV pred2){
	if (pred1.value == pred2.value){
		if(pred1.rel == LE && (pred2.rel == EQ || pred2.rel == LE || pred2.rel == GE))
			return false;
		if(pred1.rel == GE && (pred2.rel == EQ || pred2.rel == LE || pred2.rel == GE))
			return false;
		if(pred1.rel == L && (pred2.rel == LE || pred2.rel == L))
			return false;
		if(pred1.rel == G &&(pred2.rel == GE || pred2.rel == G))
			return false;
		//cout << pred1.get_str()+","+pred2.get_str() + ", conflict"<< endl;
		return true;
	}
	if (pred1.value > pred2.value){
		if (pred1.rel==L||pred1.rel==LE)
			return false;
		if ((pred1.rel==G||pred1.rel==GE||pred1.rel==EQ) && (pred2.rel==GE||pred2.rel==G))
			return false;
		//cout << pred1.get_str()+","+pred2.get_str() + ", conflict"<< endl;
		return true;
	}
	if (pred1.value < pred2.value){
		if (pred1.rel == G || pred1.rel == GE)
			return false;
		if ((pred1.rel==L||pred1.rel==LE||pred1.rel==EQ) && (pred2.rel==L||pred2.rel==LE))
			return false;
		//cout << pred1.get_str()+","+pred2.get_str() + ", conflict"<< endl;
		return true;
	}
}

bool conflict_pred(predicateS pred1, predicateS pred2){
	if(pred1.table_name != pred2.table_name || pred1.attr_name != pred2.attr_name || pred1.value != pred2.value)
		return false;
	if (pred1.rel == pred2.rel)
		return false;
	return true;
}

#endif
