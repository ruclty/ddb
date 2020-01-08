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
	bool is_key = 0;
	int size;
	string dist_name; //属性是如何分布的-分布类型
	double mean;//均值
	double std;//方差
};

//分片信息的数据结构
struct frag_info
{	
	int frag_id; 
	FRAGTYPE frag_type;
	string table_name; 
	int site_id; 
	vector<frag_info> h_frag;
	vector<frag_info> v_frag;
	vector<predicateS> preds;
	vector<predicateV> predv;
	vector<attr_info> attributes; 
	vector<string> attr_names;
	int size;
    bool is_temp;
};

//表相关信息的数据结构
struct table_info
{
	string table_name;
	vector<attr_info> attributes; 
	vector<frag_info> h_frag; 
	vector<frag_info> v_frag;
    bool is_temp;
};




frag_info fragment [MAX_FRAG_COUNT]; //fragment数组中存放有多少个如此的分片

class GDD
{
	private:
		map<int, site_info> SITES;
		map<string, table_info> TABLES;
		map<int, frag_info> FRAGMENTS;
        map<int, frag_info> TEMPTABLES;
        map<string, attr_info> ATTRIBUTES;
	public:
		site_info get_site_info(int);
		frag_info get_frag_info(int);
		table_info get_table_info(string);
		attr_info get_attr_info(string);
		void update_frag_info(int frag_id,vector<string> attr_names,vector<attr_info> attributes,int size);
		
		void generate_gdd(void); // 生成一个临时的gdd用于测试
		void print(void);

        int from_name_find_frag_id(string );
		int get_new_frag_id();
		void create_frag_info(frag_info);
		void print_frag_info(frag_info); 
};

//写两个函数：通过fragment找site、找table
site_info get_site_info(int site_id){}
frag_info get_frag_info(int frag_id){}
table_info get_table_info(string table_name) {}



#endif
