//
// Created by dyj on 2019/12/27.
//
#include "rpc/client.h"
#include "thread"
//#include "global.h"
#include "rpc_sent.h"

string mapIdtoIp(int id)
{
    std::map<int ,string> maplive;
    maplive[1]="127.0.0.1";//map中最简单最常用的插入添加！
    maplive[2]="127.0.0.1";
    maplive[3]="127.0.0.1";

    std::map<int ,string >::iterator l_it;;
    l_it=maplive.find(id);
    if(l_it==maplive.end())
        cout<<"we do not find 112"<<endl;
    return l_it->second;
}
string EnumToString(OPERATORTYPE ope)
{
    switch(ope)
    {
        case ESS: return "ESS";break;
        case SAT: return "SAT";break;
        case CRT: return "CRT";break;
        case RATT: return "RATT";break;
        case LD: return "LD";break;
        case REC: return "REC";break;
    }
}
bool SendPlan(vector<Operator> plan, int target_site_id)
{
    string results = "";
    for(int i=0; i<plan.size();i++){
        //Operator -> string
        string plans = "";
        plans.append(plan[i].content);
        plans.append("#");
        string ope=EnumToString(plan[i].ope);
        plans.append(ope);
        plans.append("#");
        plans.append(to_string(plan[i].result_frag_id));
        plans.append("#");
        plans.append(to_string(plan[i].target_site_id));
        plans.append("#");
        plans.append(to_string(plan[i].is_end));
        plans.append("#");
        for(int j=0; j<plan[i].table_names.size(); j++){
            plans.append(plan[i].table_names[j]);
            if(j != plan[i].table_names.size()-1)
                plans.append("#");
        }

        results.append(plans);
        if(i != plan.size()-1)
            results.append("$");
    }

    //call
    string target_site_ip=mapIdtoIp(target_site_id);
    int target_port=8080;
    rpc::client client(target_site_ip,target_port);
    bool ok = client.call("rpc_rec->ReceivePlan",results).as<bool>();
    cout <<  "return ok\t" << ok  << endl;
    return ok;
}
bool SendTable(int frag_id, string frag_content, int target_site_id)
{
    string target_site_ip=mapIdtoIp(target_site_id);
    int target_port=8080;
    rpc::client client(target_site_ip,target_port);
    bool ok = client.call("rpc_rec->ReceiveTable",frag_id, frag_content).as<bool>();
    cout <<  "return ok\t" << ok  << endl;
    return ok;
}

bool SendResult(int frag_id, string frag_content, int target_site_id)
{
    string target_site_ip=mapIdtoIp(target_site_id);
    int target_port=8080;
    rpc::client client(target_site_ip,target_port);
    bool ok = client.call("rpc_rec->excute_result_table",frag_id, frag_content).as<bool>();
    cout <<  "return ok\t" << ok  << endl;
    return ok;
}
