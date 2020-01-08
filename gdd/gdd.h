#ifndef _GDD_H_
#define _GDD_H_

#define MAX_SITE_COUNT 10
#define MAX_FRAG_COUNT 100

#include "../global.h"
#include <map>
#include <vector>


enum FRAGTYPE{H,V}; //分片划分依据
//站点信息数据结构
struct site_info
{
	int site_id;
	vector<int> fragment_ids; 
	vector<int> temp_ids;
    string user;
    string password;
	string ip; 
	string port; 
};

vector<site_info> site[MAX_SITE_COUNT]; 

//表的属性信息数据结构
struct attr_info
{
	string attr_name;
	string type;
	bool is_key;
};

//分片信息的数据结构
struct frag_info
{	
	int frag_id; 
	FRAGTYPE frag_type;
	string table_name; 
	int site_id; 
    bool is_temp;
    int size;
	vector<predicateS> preds;
	vector<predicateV> predv;
	vector<string> attr_names;
};

//表相关信息的数据结构
struct table_info
{
	string table_name;
	string key;
	bool is_temp;
	vector<string> attr_names; 
	map<string, attr_info> attributes;
	vector<int> h_frags; 
	vector<int> v_frags;
};

string etcd_get_value(string);
int etcd_set_value(char *key, char *value, char *token);
int etcd_set_dir(char* key, char *value, char *token);
string create_dir(string &etcd_url, string &etcd_dir);
string search_value(string &dir);

bool save_site_info(site_info site);
site_info get_site_info(int site_id);
bool save_table_info(table_info);
table_info get_table_info(string);
bool save_attr_info(string,attr_info);
attr_info get_attr_info(string, string);
bool save_frag_info(frag_info);
frag_info get_frag_info(int);
bool update_frag_info(int,vector<string>, int);

int get_frag_num();
int get_new_frag_id();
bool update_frag_num();


#endif
