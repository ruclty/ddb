//
// Created by dyj on 2019/12/27.
//

#ifndef _RPC_SENT_CPP_
#define _RPC_SENT_CPP_

//#include "thread"
//#include "global.h"
#include "rpc_sent.h"
#include <arpa/inet.h>

//#define BUFFER_SIZE 1000000


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
//void SendPlan(vector<Operator> plan, int target_site_id, int sourceId)
//{
//    string results = "";
//    cout <<  "start SendPlan \t" << endl;
//    cout <<  "plan.size() :\t" << plan.size() <<endl;
//    for(int i=0; i<plan.size();i++){
//        //Operator -> string
//        string plans = "0";
//        plans.append(plan[i].content);
//        plans.append("#");
//        string ope=EnumToString(plan[i].ope);
//        plans.append(ope);
//        plans.append("#");
//        plans.append(to_string(plan[i].result_frag_id));
//        plans.append("#");
//        plans.append(to_string(plan[i].target_site_id));
//        plans.append("#");
//        plans.append(to_string(plan[i].is_end));
//        plans.append("#");
//        for(int j=0; j<plan[i].table_names.size(); j++){
//            plans.append(plan[i].table_names[j]);
//            if(j != plan[i].table_names.size()-1)
//                plans.append("#");
//        }
//        results.append(plans);
//       // cout <<  "SendResults :\t" << results <<endl;
//        if(i != plan.size()-1)
//            results.append("$");
//    }
//    //call
//    string target_site_ip=mapIdtoIp(target_site_id, sourceId);
//    cout << "sent_len:" << results.size() << endl;
//    socket_client(target_site_id,results,sourceId);
//}
static void *SendPlan(void *arg)
{
	transfer_plan_para *para;
	para = (struct transfer_plan_para *) arg;
	vector<Operator> plan = para->plan;
	int target_site_id = para->target_site;
	int sourceId = para->source_site;
	
    string results = "";
    cout <<  "start SendPlan \t" << endl;
    cout <<  "plan.size() :\t" << plan.size() <<endl;
    for(int i=0; i<plan.size();i++){
        //Operator -> string
        string plans = "0";
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
       // cout <<  "SendResults :\t" << results <<endl;
        if(i != plan.size()-1)
            results.append("$");
    }
    //call
    string target_site_ip=mapIdtoIp(target_site_id, sourceId);
    cout << "sent_len:" << results.size() << endl;
    socket_client(target_site_id,results,sourceId);
}

void SendTable(int frag_id, string frag_content, string origin_table_name, int target_site_id, int sourceId){
	
    cout <<  "start SendTable \t" << endl;
    string target_site_ip=mapIdtoIp(target_site_id, sourceId);
    string frag_content_and_frag_id;
    string str=to_string(frag_id);

    frag_content_and_frag_id='1'+str+'#' + origin_table_name+'#'+frag_content ;
	cout << frag_content_and_frag_id << "sent_content"<< endl;
   // cout << frag_content_and_frag_id << endl;
      //  cout << origin_table_name << endl;
    //    cout << str << endl;
    cout << target_site_id << endl;
    cout << sourceId << endl;
    cout << ">>>>>" << endl;
    //cout <<  "frag_content_and_frag_id\t" << frag_content_and_frag_id << endl;
    socket_client(target_site_id,frag_content_and_frag_id,sourceId);
}

void socket_client(int target_site_id,string results,int sourceId)
{
	cout << "??socketc" << endl;
    string target_site_ip=mapIdtoIp(target_site_id, sourceId);
    cout << "socket_client"<<endl;
    char **argv;
    argv = new char*[1];
    argv[0] = new char[255];
    strcpy(argv[0],target_site_ip.c_str());
    cout << "target_site_ip.c_str()"<< argv[0]<<endl;
    // 设置一个socket地址结构client_addr, 代表客户机的internet地址和端口
    struct sockaddr_in client_addr;
    bzero(&client_addr, sizeof(client_addr));
  //  cout << "Is wrong? 1"<< argv[0]<<endl;
    client_addr.sin_family = AF_INET; // internet协议族
    //cout << "Is wrong? 2"<< argv[0]<<endl;
    client_addr.sin_addr.s_addr = htons(INADDR_ANY); // INADDR_ANY表示自动获取本机地址
//    cout << "Is wrong? 3"<< argv[0]<<endl;
    client_addr.sin_port = htons(0);
//	cout << "Is wrong? 4"<< argv[0]<<endl;
    // 创建用于internet的流协议(TCP)类型socket，用client_socket代表客户端socket
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
//    cout << "Is wrong? 5"<< argv[0]<<endl;
    if (client_socket < 0)
    {
        printf("Create Socket Failed!\n");
        exit(1);
    }
//    cout << "Is wrong? 6"<< argv[0]<<endl;
    // 把客户端的socket和客户端的socket地址结构绑定
    if (bind(client_socket,(struct sockaddr*)&client_addr, sizeof(client_addr)))
    {
        printf("Client Bind Port Failed!\n");
        exit(1);
    }
//    cout << "Is wrong? 7"<< argv[0]<<endl;
    // 设置一个socket地址结构server_addr,代表服务器的internet地址和端口
    struct sockaddr_in  server_addr;
    bzero(&server_addr, sizeof(server_addr));
//    cout << "Is wrong? 8"<< argv[0]<<endl;
    server_addr.sin_family = AF_INET;
//    cout << "Is wrong? 9"<< argv[0]<<endl;
    // 服务器的IP地址来自程序的参数
    if (inet_aton(argv[0], &server_addr.sin_addr) == 0)
    {
        printf("Server IP Address Error!\n");
        exit(1);
    }
//    cout << "Is wrong? 10"<< argv[0]<<endl;
    server_addr.sin_port = htons(SERVER_PORT[target_site_id]); 
//    cout << "Is wrong? 11"<< argv[0]<<endl;
    socklen_t server_addr_length = sizeof(server_addr);
//    cout << "Is wrong? 12"<< argv[0]<<endl;
    // 向服务器发起连接请求，连接成功后client_socket代表客户端和服务器端的一个socket连接
    if (connect(client_socket, (struct sockaddr*)&server_addr, server_addr_length) < 0)
    {
        printf("Can Not Connect To %s!\n", argv[0]);
        exit(1);
    }
    char buffer[BUFFER_SIZE];
    bzero(buffer, sizeof(buffer));
//    cout << "Is wrong? 13"<< argv[0]<<endl;
    strncpy(buffer, results.c_str(), results.length()  > BUFFER_SIZE ? BUFFER_SIZE : results.length());
//    cout << "Is wrong? 14"<< argv[0]<<endl;
    send(client_socket, buffer, BUFFER_SIZE, 0);
//    cout << "Is wrong? 16"<< argv[0]<<endl;
    // 关闭与服务器端的连接
    close(client_socket);
}

void SendResultTable(int frag_id, string frag_content, string origin_table_name,  int target_site_id, int sourceId)
{
    string target_site_ip=mapIdtoIp(target_site_id, sourceId);
    string frag_content_and_frag_id;
    string str=to_string(frag_id);
    frag_content_and_frag_id='2'+str+'#'+frag_content +'#' +origin_table_name ;
 //   cout <<  "frag_content_and_frag_id\t" << frag_content_and_frag_id << endl;
 	cout << "sent_len:" << frag_content_and_frag_id.size() << endl;
    socket_client(target_site_id,frag_content_and_frag_id,sourceId);
/*
    rpc::client client(target_site_ip,target_port);
    bool ok = client.call("excute_result_table",frag_id, frag_content).as<bool>();
    cout <<  "return ok\t" << ok  << endl;
    return ok;*/
}

void SendDeleteTemp(int target_site_id,int sourceId)
{
	string target_site_ip=mapIdtoIp(target_site_id, sourceId);
    	string frag_content_and_frag_id;
    	
    frag_content_and_frag_id='3';
 //   cout <<  "frag_content_and_frag_id\t" << frag_content_and_frag_id << endl;
 	cout << "sent_len:" << frag_content_and_frag_id.size() << endl;
    socket_client(target_site_id,frag_content_and_frag_id,sourceId);
	}
#endif