#ifndef _GDD_CPP_
#define _GDD_CPP_

#include "gdd.h"
#include <iostream>
#include <curl/curl.h>
#include <json/json.h>
#include <stdio.h>
#include <cstdlib>
using namespace std;

#define URL_MAX_LEN 1024
#define VALUE_LEN 1024

string frag_type_to_str(FRAGTYPE f)
{
    string str = (f==H)?"H":"V";
    return str;
}

FRAGTYPE str_to_frag_type(string str)
{
    FRAGTYPE f = (str=="H")?H:V;
    return f;
}

int from_name_find_frag_id(string table_name){
	vector<string> spl = split(table_name,"_");
	return atoi(spl[spl.size()-1].c_str());
	}

void print_site(site_info s){
    cout << "Sites:" <<s.site_id<< endl;
    cout << s.ip <<" "<<s.port<<" "<<s.user<<" "<<s.password<< endl;
    cout <<"fragment_ids: " ;
    for (auto f:s.fragment_ids)
          cout << f << ", ";
    cout << endl;
    cout <<"temp_ids: " ;
    for (auto f:s.temp_ids)
          cout << f << ", ";
    cout << endl;
    cout << endl;
}

void print_attr_info(attr_info attr)
{
    cout << attr.attr_name << ","<<attr.type<<","<<attr.is_key;
}

void print_table(table_info t){
    cout << "Tables:" <<t.table_name <<endl;
    cout << "key:" << t.key << endl;
    cout << "is_temp:" << t.is_temp <<endl;
    cout << "attr_names: ";
    for (auto f:t.attr_names)
        cout << f << ", ";
    cout <<endl;
    for (auto a:t.attributes){
        print_attr_info(a.second);
        cout << endl;
    }
    cout << "h frags: ";
    for (auto f:t.h_frags)
        cout << f << ", ";
    cout << endl;
    cout << "v frags: ";
    for (auto f:t.v_frags)
        cout << f << ", ";
    cout << endl;
    cout << endl;
}

void print_frag(frag_info f)
{
    cout << "Fragment: "<<f.frag_id << ", "<<f.table_name << ", " << f.site_id << ", " << frag_type_to_str(f.frag_type) <<", "<< f.is_temp <<", "<<f.size << endl;
    if(f.frag_type == H){
        for(auto p: f.predv)
            cout << p.get_str() << "|";
    }
    //else{
    for(auto a: f.attr_names)
        cout << a << "|";
    cout << endl;
    for(int i = f.attr_infos.size();i >= 1; i --)
    {
        print_attr_info(f.attr_infos[i]);
        cout << endl;
    }
    //}

    cout << endl;
    cout << endl;
}
/*
void GDD::update_frag_info(int frag_id,vector<string> attr_names,vector<attr_info> attributes,int size){
    this->FRAGMENTS[frag_id].attr_names = attr_names;
    this->FRAGMENTS[frag_id].size = size;
}

}*/

string get_frag_name(int frag_id)
{
	string name;
	frag_info info = get_frag_info(frag_id);
	if(info.is_temp == true)
		name = "temp_table_";
    else
    	     name = "frag_";
    name = name + to_string(frag_id);
    return name;
}

string etcd_get_value(string key)
{
    Json::Value root;
    Json::Value node;
    Json::Reader reader;
    Json::FastWriter writer;
    string info;

    info = search_value(key);
    reader.parse(info, root);
    node = root["node"];
    string value = writer.write(node["value"]);
    value = value.substr(1, value.length()-3);
    return value;
}

int etcd_set_dir(char *key, char *value, char *token)  
{  //向etcd创建dir数据
    CURLcode return_code;  
    char etcd_url[URL_MAX_LEN];  
    char etcd_value[VALUE_LEN];   
      
    return_code = curl_global_init(CURL_GLOBAL_SSL);  
    if (CURLE_OK != return_code)  
    {  
        //cerr << "init libcurl failed." << endl;  
        printf("init libcurl failed\n");  
        return -1;  
    }  
  
    sprintf(etcd_url, "http://127.0.0.1:2379/v2/keys%s", key);  
    sprintf(etcd_value, "dir=%s", value);  
    // 获取easy handle  
    CURL *easy_handle = curl_easy_init();  
    if (NULL == easy_handle)  
    {  
        //cerr << "get a easy handle failed." << endl;  
        printf("get a easy handle failed.\n");  
        curl_global_cleanup();   
        return -1;  
    }  
    // 设置easy handle属性  
    curl_easy_setopt(easy_handle, CURLOPT_URL, etcd_url);   
    curl_easy_setopt(easy_handle, CURLOPT_POST, 1);  
    curl_easy_setopt(easy_handle, CURLOPT_POSTFIELDS, etcd_value);  
    curl_easy_setopt(easy_handle, CURLOPT_CUSTOMREQUEST, "PUT");  
   
    // 执行数据请求  
    curl_easy_perform(easy_handle);   
  
    // 释放资源  
    curl_easy_cleanup(easy_handle);  
    curl_global_cleanup();  
    return 0;  
} 

size_t write_data(void *buffer, size_t size, size_t nmemb, void *stream) 
//从etcd里面取得部分数据
{ 
    strncat((char*)stream,(char*)buffer,size*nmemb);
    return nmemb*size; 
} 

int etcd_set_value(char *key, char *value, char *token)  
//向etcd存储value数据
{    
    CURLcode return_code;   
    char etcd_value[VALUE_LEN];   
      
    return_code = curl_global_init(CURL_GLOBAL_SSL);  
    if (CURLE_OK != return_code)  
    {  
        //cerr << "init libcurl failed." << endl;  
        printf("init libcurl failed\n");  
        return -1;  
    }  
    string a = key;
    string str = "http://127.0.0.1:2379/v2/keys"+a;
    char *ss=(char*)str.c_str();
    sprintf(etcd_value, "value=%s", value);  
    // 获取easy handle  
    CURL *easy_handle = curl_easy_init();  
    if (NULL == easy_handle)  
    {  
        //cerr << "get a easy handle failed." << endl;  
        printf("get a easy handle failed.\n");  
        curl_global_cleanup();   
        return -1;  
    }  
    // 设置easy handle属性  
    curl_easy_setopt(easy_handle, CURLOPT_URL, ss);   
    curl_easy_setopt(easy_handle, CURLOPT_POST, 1);  
    curl_easy_setopt(easy_handle, CURLOPT_POSTFIELDS, etcd_value);  
    curl_easy_setopt(easy_handle, CURLOPT_CUSTOMREQUEST, "PUT");  
   
    // 执行数据请求  
    curl_easy_perform(easy_handle);   
  
    // 释放资源  
    curl_easy_cleanup(easy_handle);  
    curl_global_cleanup();  
    return 0;  
}  

string create_dir(string &etcd_url,string &etcd_dir)
{ //在etcd里面创建dir
    string str=etcd_url+etcd_dir;
    cout<<str<<endl;
    char *p=(char*)str.c_str();
    etcd_set_dir(p, "true", NULL);  
    return str; 
}

bool insert_value(string &key,string &value)
{  //调用存储数据的etcd接口  

    string str=key;
    char *p=(char*)str.c_str();
    char *data=(char*)value.c_str();
    etcd_set_value(p, data, NULL);  
    return true; 
}

string search_value(string &dir)
//将全部数据合并
{
    string etcd_url = "http://127.0.0.1/v2/keys"+dir+"?recursive=true";
    char *ss=(char*)etcd_url.c_str();
    CURLcode return_code;  
    return_code = curl_global_init(CURL_GLOBAL_SSL);  
    if (CURLE_OK != return_code)  
    {  
        cerr << "init libcurl failed." << endl;  
        return "";  
    }  
    
    // 获取easy handle  
    CURL *easy_handle = curl_easy_init();  
    if (NULL == easy_handle)  
    {  
        cerr << "get a easy handle failed." << endl;  
        curl_global_cleanup();   
  
        return "";  
    }  
  
    char * buff_p = NULL;  
    char result[MAX_LEN] = "";
    // 设置easy handle属性  
    curl_easy_setopt(easy_handle, CURLOPT_URL,"http://127.0.0.1/v2/keys/fragment_info/frag_1/Size"); 
    curl_easy_setopt(easy_handle, CURLOPT_PORT, 2379);  
    curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, &write_data);  
    curl_easy_setopt(easy_handle, CURLOPT_WRITEDATA, &result);  
  
    // 执行数据请求  
  
    curl_easy_perform(easy_handle);   
    // 释放资源  
  
    curl_easy_cleanup(easy_handle);  
    curl_global_cleanup();  
    string info = result;
    return info;
}

bool save_site_info(site_info site)
{
    string key;
    string value;
    string dir = "/"+to_string(site.site_id);
    string url = "/siteinfo";
    url = create_dir(url, dir);



    key = url + "/site_id";
    value = to_string(site.site_id);
    insert_value(key, value);
    
    key = url + "/ip";
    value = site.ip;
    insert_value(key, value);

    key = url + "/port";
    value = site.port;
    insert_value(key, value);

    key = url + "/user";
    value = site.user;
    insert_value(key, value);

    key = url + "/password";
    value = site.password;
    insert_value(key, value);

    dir = "/fragment_ids";
    string frag_dir = create_dir(url, dir);
    dir = "/temp_ids";
    string temp_dir = create_dir(url, dir);
    for(int i = 0; i < site.fragment_ids.size(); i ++){
        key = frag_dir + "/" +to_string(site.fragment_ids[i]);
        value = to_string(site.fragment_ids[i]);
        insert_value(key, value);
    }

    for(int i = 0; i < site.temp_ids.size(); i ++){
        key = temp_dir + "/" +to_string(site.temp_ids[i]);
        value = to_string(site.temp_ids[i]);
        insert_value(key, value);
    }

    return true;
}

site_info get_site_info(int site_id)
{
    site_info site = site_info{site_id};
    Json::Value root;
    Json::Value node;
    Json::Reader reader;
    Json::FastWriter writer;
    string dir;
    string info;

    dir = "/siteinfo/"+to_string(site_id)+"/ip";
    site.ip = etcd_get_value(dir);

    dir = "/siteinfo/"+to_string(site_id)+"/port";
    site.port = etcd_get_value(dir);

    dir = "/siteinfo/"+to_string(site_id)+"/user";
    site.user = etcd_get_value(dir);

    dir = "/siteinfo/"+to_string(site_id)+"/password";
    site.password = etcd_get_value(dir);

    dir = "/siteinfo/"+to_string(site_id)+"/fragment_ids";
    info = search_value(dir);
    reader.parse(info, root);
    node = root["node"]["nodes"];
    vector<int> fragment_ids;
    for (int i = 0; i < node.size();i ++){
        string id = writer.write(node[i]["value"]);
        id = id.substr(1, id.length()-3);
        fragment_ids.push_back(stoi(id));
    }
    site.fragment_ids = fragment_ids;

    dir = "/siteinfo/"+to_string(site_id)+"/temp_ids";
    info = search_value(dir);
    reader.parse(info, root);
    node = root["node"]["nodes"];
    vector<int> temp_ids;
    for (int i = 0; i < node.size();i ++){
        string id = writer.write(node[i]["value"]);
        id = id.substr(1, id.length()-3);
        temp_ids.push_back(stoi(id));
    }
    site.temp_ids = temp_ids;
    
    return site;
}

bool save_table_info(table_info table)
{
    string key;
    string value;
    string dir = "/"+table.table_name;
    string url = "/tableinfo";
    url = create_dir(url, dir);

    key = url+"/table_name";
    value = table.table_name;
    insert_value(key, value);

    key = url+"/key";
    value = table.key;
    insert_value(key, value);

    key = url+"/is_temp";
    cout << table.is_temp << endl;
    value = table.is_temp?"true":"false";
    insert_value(key, value);

    dir = "/attr_names";
    string attr_name_dir = create_dir(url, dir);
    for(auto a: table.attr_names){
        key = attr_name_dir+"/"+a;
        value = a;
        insert_value(key, value);
    }

    dir = "/v_frags";
    string v_frag_dir = create_dir(url, dir);
    for(auto v: table.v_frags){
        key = v_frag_dir+"/" + to_string(v);
        value = to_string(v);
        insert_value(key, value);
    }

    dir = "/h_frags";
    string h_frag_dir = create_dir(url, dir);
    for(auto h: table.h_frags){
        key = h_frag_dir+"/"+to_string(h);
        value = to_string(h);
        insert_value(key, value);
    }

    dir = "/attributes";
    create_dir(url, dir);
    for(auto a: table.attributes){
        save_attr_info(table.table_name, a.second);
    }
    return true;
}

table_info get_table_info(string table_name)
{
    table_info table = table_info{table_name};
    Json::Value root;
    Json::Value node;
    Json::Reader reader;
    Json::FastWriter writer;
    string dir;
    string info;

    dir = "/tableinfo/"+table_name+"/key";
    table.key = etcd_get_value(dir);

    dir = "/tableinfo/"+table_name+"/is_temp";
    string is_temp = etcd_get_value(dir);
    table.is_temp = (is_temp == "true")?true:false;

    dir = "/tableinfo/"+table_name+"/h_frags";
    info = search_value(dir);
    reader.parse(info, root);
    node = root["node"]["nodes"];
    vector<int> h_frags;
    for (int i = 0; i < node.size();i ++){
        string id = writer.write(node[i]["value"]);
        id = id.substr(1, id.length()-3);
        h_frags.push_back(stoi(id));
    }
    table.h_frags = h_frags;

    dir = "/tableinfo/"+table_name+"/v_frags";
    info = search_value(dir);
    reader.parse(info, root);
    node = root["node"]["nodes"];
    vector<int> v_frags;
    for (int i = 0; i < node.size();i ++){
        string id = writer.write(node[i]["value"]);
        id = id.substr(1, id.length()-3);
        v_frags.push_back(stoi(id));
    }
    table.v_frags = v_frags;

    dir = "/tableinfo/"+table_name+"/attr_names";
    info = search_value(dir);
    reader.parse(info, root);
    node = root["node"]["nodes"];
    vector<string> attr_names;
    for (int i = 0; i < node.size();i ++){
        string name = writer.write(node[i]["value"]);
        name = name.substr(1, name.length()-3);
        attr_names.push_back(name);
    }
    table.attr_names = attr_names;

    map<string, attr_info> attributes;
    for (auto attr_name: table.attr_names){
        attr_info attr = get_attr_info(table.table_name, attr_name);
        attributes[attr_name] = attr;
    }
    table.attributes = attributes;
    return table;
}

bool save_attr_info(string table_name, attr_info attr)
{
    string key;
    string value;
    string dir = "/"+attr.attr_name;
    string url = "/tableinfo/"+table_name+"/attributes";
    url = create_dir(url, dir);

    key = url+"/name";
    value = attr.attr_name;
    insert_value(key, value);

    key = url+"/type";
    value = attr.type;
    insert_value(key, value);

    key = url+"/is_key";
    value = attr.is_key?"true":"false";
    insert_value(key, value);

    return true;
}

bool save_attr_info(int frag_id, int index, attr_info attr)
{
    string key;
    string value;
    string dir = "/"+attr.attr_name;
    string url = "/fraginfo/"+to_string(frag_id)+"/attributes";
    url = create_dir(url, dir);

    key = url+"/name";
    value = attr.attr_name;
    insert_value(key, value);

    key = url+"/type";
    value = attr.type;
    insert_value(key, value);

    key = url+"/index";
    value = to_string(index);
    insert_value(key, value);

    key = url+"/is_key";
    value = attr.is_key?"true":"false";
    insert_value(key, value);

    return true;
}

attr_info get_attr_info(string table_name, string attr_name)
{
    attr_info attr = attr_info{attr_name};
    Json::Value root;
    Json::Value node;
    Json::Reader reader;
    Json::FastWriter writer;
    string dir;
    string info;

    dir = "/tableinfo/"+table_name+"/attributes/"+attr_name+"/type";
    attr.type = etcd_get_value(dir);

    dir = "/tableinfo/"+table_name+"/attributes/"+attr_name+"/is_key";
    string is_key = etcd_get_value(dir);
    attr.is_key = (is_key=="true")?true:false;

    return attr;
}

attr_info get_attr_info(int frag_id, string attr_name)
{
    attr_info attr = attr_info{attr_name};
    Json::Value root;
    Json::Value node;
    Json::Reader reader;
    Json::FastWriter writer;
    string dir;
    string info;

    dir = "/fraginfo/"+to_string(frag_id)+"/attributes/"+attr_name+"/type";
    attr.type = etcd_get_value(dir);

    dir = "/fraginfo/"+to_string(frag_id)+"/attributes/"+attr_name+"/is_key";
    string is_key = etcd_get_value(dir);
    attr.is_key = (is_key=="true")?true:false;

    return attr;
}

bool save_frag_info(frag_info frag)
{
    string key;
    string value;
    string dir = "/"+to_string(frag.frag_id);
    string url = "/fraginfo";
    url = create_dir(url, dir);

    key = url+"/frag_id";
    value = to_string(frag.frag_id);
    insert_value(key, value);

    key = url+"/frag_type";
    value = frag_type_to_str(frag.frag_type);
    insert_value(key, value);

    key = url+"/table_name";
    value = frag.table_name;
    insert_value(key, value);

    key = url+"/site_id";
    value = to_string(frag.site_id);
    insert_value(key, value);

    key = url+"/is_temp";
    value = frag.is_temp?"true":"false";
    insert_value(key, value);

    key = url+"/size";
    value = to_string(frag.size);
    insert_value(key, value);

    string tab_dir;
    string site_dir;

    if(frag.frag_type == H)
        tab_dir = "/tableinfo/"+frag.table_name+"/h_frags";
    else
        tab_dir = "/tableinfo/"+frag.table_name+"/v_frags";
    key = tab_dir+"/"+to_string(frag.frag_id);
    value = to_string(frag.frag_id);
    insert_value(key, value);

    if(frag.is_temp)
        site_dir = "/siteinfo/"+to_string(frag.site_id)+"/temp_ids";
    else
        site_dir = "/siteinfo/"+to_string(frag.site_id)+"/fragment_ids";
    key = site_dir + "/"+to_string(frag.frag_id);
    cout << key <<endl;
    value = to_string(frag.frag_id);
    insert_value(key, value);
        
    dir = "/preds";
    string preds_dir = create_dir(url, dir);
    for(int i = 0;i < frag.preds.size() ;i ++){
        dir = "/preds_"+to_string(i);
        string new_preds = create_dir(preds_dir, dir);
        key = new_preds + "/table_name";
        value = frag.preds[i].table_name;
        insert_value(key, value);

        key = new_preds + "/attr_name";
        value = frag.preds[i].attr_name;
        insert_value(key, value);

        key = new_preds+"/rel";
        value = rela_to_token(frag.preds[i].rel);
        insert_value(key, value);

        key = new_preds+"/value";
        value = frag.preds[i].value;
        insert_value(key, value);
    }

    dir = "/predv";
    string predv_dir = create_dir(url, dir);
    for(int i = 0;i < frag.predv.size();i ++){
        dir = "/predv_"+to_string(i);
        string new_predv = create_dir(predv_dir, dir);
        key = new_predv + "/table_name";
        value = frag.predv[i].table_name;
        insert_value(key, value);

        key = new_predv + "/attr_name";
        value = frag.predv[i].attr_name;
        insert_value(key, value);

        key = new_predv+"/rel";
        value = rela_to_token(frag.predv[i].rel);
        insert_value(key, value);

        key = new_predv+"/value";
        value = to_string(frag.predv[i].value);
        insert_value(key, value);
    }

    dir = "/attr_names";
    string attr_dir = create_dir(url, dir);
    for(auto a: frag.attr_names){
        key = attr_dir+"/"+a;
        value = a;
        insert_value(key, a);
    }

    dir = "/attributes";
    create_dir(url, dir);
    for(auto a: frag.attr_infos){
        save_attr_info(frag.frag_id, a.first, a.second);
    }

    update_frag_num();
    return true;
}

frag_info get_frag_info(int frag_id)
{
    frag_info frag = frag_info{frag_id};
    Json::Value root;
    Json::Value node;
    Json::Reader reader;
    Json::FastWriter writer;
    string dir;
    string info;

    dir = "/fraginfo/"+to_string(frag_id)+"/frag_type";
    string type = etcd_get_value(dir);
    frag.frag_type = str_to_frag_type(type);

    dir = "/fraginfo/"+to_string(frag_id)+"/table_name";
    frag.table_name = etcd_get_value(dir);

    dir = "/fraginfo/"+to_string(frag_id)+"/site_id";
    string site_id = etcd_get_value(dir);
    frag.site_id = stoi(site_id);

    dir = "/fraginfo/"+to_string(frag_id)+"/is_temp";
    string is_temp = etcd_get_value(dir);
    frag.is_temp = (is_temp=="true")?true:false;

    dir = "/fraginfo/"+to_string(frag_id)+"/size";
    string size = etcd_get_value(dir);
    frag.size = stoi(size);

    dir = "/fraginfo/"+to_string(frag_id)+"/attr_names";
    info = search_value(dir);
    reader.parse(info, root);
    node = root["node"]["nodes"];
    vector<string> attr_names;
    for (int i = 0; i < node.size();i ++){
        string name = writer.write(node[i]["value"]);
        name = name.substr(1, name.length()-3);
        attr_names.push_back(name);
    }
    frag.attr_names = attr_names;

    dir = "/fraginfo/"+to_string(frag_id)+"/predv";
    info = search_value(dir);
    reader.parse(info, root);
    node = root["node"]["nodes"];
    vector<predicateV> predv;
    for (int i = 0; i < node.size();i ++){
        string key;
        predicateV pred;
        string pred_dir = writer.write(node[i]["key"]);
        pred_dir = pred_dir.substr(1, pred_dir.length()-3);
        
        key = pred_dir+"/table_name";
        pred.table_name = etcd_get_value(key);

        key = pred_dir+"/attr_name";
        pred.attr_name = etcd_get_value(key);

        key = pred_dir+"/rel";
        pred.rel = token_to_rela(etcd_get_value(key));

        key = pred_dir+"/value";
        pred.value = stoi(etcd_get_value(key));
        predv.push_back(pred);
    }
    frag.predv = predv;

    dir = "/fraginfo/"+to_string(frag_id)+"/preds";
    info = search_value(dir);
    reader.parse(info, root);
    node = root["node"]["nodes"];
    vector<predicateS> preds;
    for (int i = 0; i < node.size();i ++){
        string key;
        predicateS pred;
        string pred_dir = writer.write(node[i]["key"]);
        pred_dir = pred_dir.substr(1, pred_dir.length()-3);
        
        key = pred_dir+"/table_name";
        pred.table_name = etcd_get_value(key);

        key = pred_dir+"/attr_name";
        pred.attr_name = etcd_get_value(key);

        key = pred_dir+"/rel";
        pred.rel = token_to_rela(etcd_get_value(key));

        key = pred_dir+"/value";
        pred.value = etcd_get_value(key);
        preds.push_back(pred);
    }
    frag.preds = preds;

    map<int, attr_info> attr_infos;
    for (auto attr_name: frag.attr_names){
        attr_info attr = get_attr_info(frag.frag_id, attr_name);
        string attr_index = "/fraginfo/"+to_string(frag_id)+"/attributes/"+attr_name+"/index";
        string index = etcd_get_value(attr_index);
        attr_infos[stoi(index)] = attr;
    }
    frag.attr_infos = attr_infos;

    return frag;
}

void generate_gdd()
{
    string dir;
    string key;
    string value;
    string url = "";

    dir = "/siteinfo";
    create_dir(url, dir);

    dir = "/fraginfo";
    create_dir(url, dir);

    dir = "/tableinfo";
    create_dir(url, dir);

    key = "/fraginfo/frag_num";
    value = "0";
    insert_value(key, value);
    
    map<int, site_info> SITES;
    map<string, table_info> TABLES;
    map<int, frag_info> FRAGMENTS;
    map<int, frag_info> TEMPTABLES;
    map<string, attr_info> ATTRIBUTES;

    site_info site;
    table_info tab;
    for(int i = 1; i <= 4;i ++){
        site_info s = site_info{i};
        s.port = "3306";
        s.user = "root";
        s.password = "ddb09890";
        SITES[i] = s;

    }

    SITES[1].ip = "10.77.70.126";
    SITES[2].ip = "10.77.70.127"; 
    SITES[3].ip = "10.77.70.126";
    SITES[4].ip = "10.77.70.127";

    save_site_info(SITES[1]);
    save_site_info(SITES[2]);
    save_site_info(SITES[3]);
    save_site_info(SITES[4]);

    cout << "fragnum: " << get_frag_num() << endl;
    site = get_site_info(1);
    print_site(site);
    site = get_site_info(2);
    print_site(site);
    site = get_site_info(3);
    print_site(site);
    site = get_site_info(4);
    print_site(site);

    table_info table1 = table_info{"student","sid",false};
    table1.attr_names.push_back("sid");
    table1.attributes["sid"] = attr_info{"sid", "int", true};
    table1.attr_names.push_back("sname");
    table1.attributes["sname"] = attr_info{"sname", "char(25)", false};
    table1.attr_names.push_back("sex");
    table1.attributes["sex"] = attr_info{"sex", "char(1)", false};
    table1.attr_names.push_back("age");
    table1.attributes["age"] = attr_info{"age", "int", false};
    table1.attr_names.push_back("degree");
    table1.attributes["degree"] = attr_info{"degree", "int", false};
    save_table_info(table1);

    table_info table2 = table_info{"teacher","tid",false};
    table2.attr_names.push_back("tid");
    table2.attributes["tid"] = attr_info{"tid", "int", true};
    table2.attr_names.push_back("tname");
    table2.attributes["tname"] = attr_info{"tname", "char(25)", false};
    table2.attr_names.push_back("title");
    table2.attributes["title"] = attr_info{"title", "int", false};
    save_table_info(table2);

    table_info table3 = table_info{"course","cid",false};
    table3.attr_names.push_back("cid");
    table3.attributes["cid"] = attr_info{"cid", "int", true};
    table3.attr_names.push_back("cname");
    table3.attributes["cname"] = attr_info{"cname", "char(80)", false};
    table3.attr_names.push_back("location");
    table3.attributes["location"] = attr_info{"location", "char(8)", false};
    table3.attr_names.push_back("credit_hour");
    table3.attributes["credit_hour"] = attr_info{"credit_hour", "int", false};
    table3.attr_names.push_back("teacher_id");
    table3.attributes["teacher_id"] = attr_info{"teacher_id", "int", false};
    save_table_info(table3);

    table_info table4 = table_info{"exam","student_id",false};
    table4.attr_names.push_back("student_id");
    table4.attributes["student_id"] = attr_info{"student_id", "int", true};
    table4.attr_names.push_back("course_id");
    table4.attributes["course_id"] = attr_info{"course_id", "int", true};
    table4.attr_names.push_back("mark");
    table4.attributes["mark"] = attr_info{"mark", "int", false};
    save_table_info(table4);

    cout << "fragnum: " << get_frag_num() << endl;
    tab = get_table_info("student");
    print_table(tab);
    tab = get_table_info("teacher");
    print_table(tab);
    tab = get_table_info("course");
    print_table(tab);
    tab = get_table_info("exam");
    print_table(tab);


    frag_info frag;
    predicateV predv;
    predicateV predv1;
    predicateV predv2;

    frag = frag_info{get_new_frag_id(),H,"student",1,false, 49999};
    predv = predicateV{"student","sid",L,1050000};
    frag.predv.push_back(predv);
    save_frag_info(frag);

    frag = frag_info{get_new_frag_id(),H,"student",2,false, 50000};
    predv1 = predicateV{"student","sid",GE,1050000};
    predv2 = predicateV{"student","sid",L,1100000};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);
    

    frag = frag_info{get_new_frag_id(),H,"student",3,false, 50001};
    predv = predicateV{"student","sid",GE,1100000};
    frag.predv.push_back(predv);
    save_frag_info(frag);

    frag = frag_info{get_new_frag_id(),H,"teacher",1, false, 8119};
    predv1 = predicateV{"teacher","tid",L,2010000};
    predv2 = predicateV{"teacher","title",NE,3};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);

    frag = frag_info{get_new_frag_id(),H,"teacher",2, false, 1880};
    predv1 = predicateV{"teacher","tid",L,2010000};
    predv2 = predicateV{"teacher","title",EQ,3};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);
    

    frag = frag_info{get_new_frag_id(),H,"teacher",3, false, 32405};
    predv1 = predicateV{"teacher","tid",GE,2010000};
    predv2 = predicateV{"teacher","title",NE,3};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);
    
    frag = frag_info{get_new_frag_id(),H,"teacher",4, false, 7596};
    predv1 = predicateV{"teacher","tid",GE,2010000};
    predv2 = predicateV{"teacher","title",EQ,3};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);
    
    frag = frag_info{get_new_frag_id(),V,"course",1,false, 2357};
    frag.attr_names.push_back("cid");
    frag.attr_names.push_back("cname");
    frag.attr_infos[1] = attr_info{"cid", "int", true};
    frag.attr_infos[2] = attr_info{"cname", "char(80)",false};
    save_frag_info(frag);
    

    frag = frag_info{get_new_frag_id(),V,"course",2,false, 2357};
    frag.attr_names.push_back("cid");
    frag.attr_names.push_back("location");
    frag.attr_names.push_back("credit_hour");
    frag.attr_names.push_back("teacher_id");
    frag.attr_infos[1] = attr_info{"cid", "int", true};
    frag.attr_infos[2] = attr_info{"location", "char(8)",false};
    frag.attr_infos[3] = attr_info{"credit_hour", "int", false};
    frag.attr_infos[4] = attr_info{"teacher_id", "int",false};
    save_frag_info(frag);
    

    //fragments of exam table
    frag = frag_info{get_new_frag_id(),H,"exam",1, false, 0};
    predv1 = predicateV{"exam","student_id",L,1070000};
    predv2 = predicateV{"exam","course_id",L,301200};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);

    frag = frag_info{get_new_frag_id(),H,"exam",2, false, 280857};
    predv1 = predicateV{"exam","student_id",L,1070000};
    predv2 = predicateV{"exam","course_id",GE,301200};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);
    

    frag = frag_info{get_new_frag_id(),H,"exam",3, false, 0};
    predv1 = predicateV{"exam","student_id",GE,1070000};
    predv2 = predicateV{"exam","course_id",L,301200};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);
    

    frag = frag_info{get_new_frag_id(),H,"exam",4, false, 319143};
    predv1 = predicateV{"exam","student_id",GE,1070000};
    predv2 = predicateV{"exam","course_id",GE,301200};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    save_frag_info(frag);

    cout << "fragnum: " << get_frag_num() << endl;
    site = get_site_info(1);
    print_site(site);
    site = get_site_info(2);
    print_site(site);
    site = get_site_info(3);
    print_site(site);
    site = get_site_info(4);
    print_site(site);

    tab = get_table_info("student");
    print_table(tab);
    tab = get_table_info("teacher");
    print_table(tab);
    tab = get_table_info("course");
    print_table(tab);
    tab = get_table_info("exam");
    print_table(tab);

    for(int i = 1;i <= 13; i ++){
        frag = get_frag_info(i);
        print_frag(frag);
    }

    cout << get_frag_num() << endl;
    print_frag(get_frag_info(8));

    cout << endl;

}

int get_frag_num()
{
    Json::Value json;
    Json::FastWriter writer;
    Json::Reader reader;
    string key = "/fraginfo/frag_num";
    string info = search_value(key);
    reader.parse(info, json);
    string num = writer.write(json["node"]["value"]);
    num = num.substr(1, num.length()-3);
    return stoi(num);
}

bool update_frag_num()
{
    int num = get_frag_num();
    num = num+1;
    string key = "/fraginfo/frag_num";
    string value = to_string(num);
    insert_value(key, value);
    return true;
}

bool update_frag_info(int frag_id, vector<string> attr_names, map<int, attr_info> attr_infos ,int size)
{
    string attr_dir = "/fraginfo/"+to_string(frag_id)+"/attr_names";
    for(auto a: attr_names){
        string key = attr_dir+"/"+a;
        string value = a;
        insert_value(key, a);
    }

    for(auto attr: attr_infos){
        save_attr_info(frag_id, attr.first, attr.second);
    }

    string key = "/fraginfo/"+to_string(frag_id)+"/size";
    string value = to_string(size);
    insert_value(key, value); 

    return true;
}

int get_new_frag_id()
{
    return get_frag_num()+1;
}

#endif
