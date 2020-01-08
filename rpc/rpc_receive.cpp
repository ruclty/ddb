//
// Created by dyj on 2019/12/8.
//
#include "rpc_receive.h"
#include <rpc/client.h>
//#include "global.h"
#include  <map>

using namespace std;
// rpc_receive::rpc_receive(int site_id){
//     this->site_id = site_id;
// //    this->site_exc = site_excution(site_id);
// }
//rpc_receive::~rpc_receive(){}
// vector<string> split(string& src,const string& separator){
//     vector<string> dest;
//     if(src == "")
//         return dest;
//     string str = src;
//     string substring;
//     int start = 0, index;
//     dest.clear();
//     index = str.find_first_of(separator,start);
//     do
//     {
//         if (index != string::npos)
//         {
//             substring = str.substr(start,index-start );
//             dest.push_back(substring);
//             start =index+separator.size();
//             index = str.find(separator,start);
//             if (start == string::npos) break;
//         }
//     }while(index != string::npos);

//     substring = str.substr(start);
//     dest.push_back(substring);
//     return dest;
// }

OPERATORTYPE mapStringtoEnum(string op)
{
    std::map<string ,OPERATORTYPE> maplive;
    maplive["ESS"]=ESS;//map中最简单最常用的插入添加！
    maplive["SAT"]=SAT;
    maplive["CRT"]=CRT;
    maplive["RATT"]=RATT;
    maplive["LD"]=LD;
    maplive["REC"]=REC;

    std::map<string ,OPERATORTYPE>::iterator l_it;;
    l_it=maplive.find(op);
    if(l_it==maplive.end())
        cout<<"we do not find 112"<<endl;
    return l_it->second;
}

bool ReceivePlan(string plans,site_excution* site_exc)
{
    vector<Operator> results;
    std::cout <<  "have received plan\t" << plans  << endl;
    vector<string> split_plans = split(plans, "$");
    for(int i=0;i<split_plans.size();i++){
        vector<string> vecplans = split(split_plans[i],"#");
        
        std::cout <<  "v.size() - 1\t" << vecplans.size() << endl;
        for (int j = 0; j< vecplans.size();j++)
        {
            std::cout <<  "vecplans\t" << vecplans[j]  << endl;
        }

        Operator plan;
        plan.content = vecplans[0];
        OPERATORTYPE ope=mapStringtoEnum(vecplans[1]);
        plan.ope = ope;
        plan.result_frag_id = std::stoi(vecplans[2]);
        plan.target_site_id = std::stoi(vecplans[3]);
        plan.is_end = std::stoi(vecplans[4]);
        
        for(int j=4;j<vecplans.size();j++)
            plan.table_names.push_back(vecplans[j]);

        results.push_back(plan);
    }

    site_exc->sql_queue = results;
    site_exc->excute_results(results);
    return true;
}


bool ReceiveTable(int frag_id, string frag_content, site_excution* site_exc)
{
    std::cout <<  "have received table\t" << frag_id  << endl;
    std::cout <<  "have received table\t" << frag_content  << endl;
    string table_name = site_exc->mysql.gdd.get_frag_info(frag_id).table_name;
    excute_rec_table(table_name, frag_content,site_exc);
    return true;
}

bool ReceiveResult(int frag_id, string frag_content, site_excution* site_exc)
{
    std::cout <<  "have received table\t" << frag_id  << endl;
    std::cout <<  "have received table\t" << frag_content  << endl;
    string table_name = site_exc->mysql.gdd.get_frag_info(frag_id).table_name;
    excute_result_table(table_name,frag_content,site_exc);
    return true;
}

void startListening(int port){
    std::cout << "StartListening" << std::endl;
    std::cout << port << std::endl;
    rpc::server srv(port);


    srv.bind("ReceivePlan",&ReceivePlan);
    srv.bind("ReceiveTable",&ReceiveTable);
    //srv.bind("ReceivePlan",&ReceivePlan);


    srv.suppress_exceptions(true);
    std::cout << "srv.run()" << std::endl;
    srv.run();
}

void excute_rec_table(string table_name, string table_content, site_excution* site_exc){
    vector<Operator> excution_plans;
    excution_plans = site_exc->recieve_and_check(table_name);

    int frag_id = site_exc->mysql.gdd.from_name_find_frag_id(table_name);
    site_exc->mysql.create_received_table(table_content,frag_id,table_name);
    site_exc->table_queue.push_back(table_name);
    site_exc->excute_results(excution_plans);
}
// vector<Operator> save_rec_plan(vector<Operator> plans){
//     vector<Operator> results;
//     for(int i=0; i < plans.size(); i++)
//         results.push_back(plans[i]);  
//     return results; 
// }
void excute_result_table(string table_name, string table_content,site_excution* site_exc){
    int frag_id = site_exc->mysql.gdd.from_name_find_frag_id(table_name);
    frag_info result_info = site_exc->mysql.gdd.get_frag_info(frag_id);
    for(int i=0;i<result_info.attr_names.size();i++)
    {
        cout << result_info.attr_names[i];
        if(i != result_info.attr_names.size()-1)
            cout << ",";
        else
            cout << endl;
    }

    cout << table_content << endl;
}
