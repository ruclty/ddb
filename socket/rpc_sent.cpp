//
// Created by dyj on 2019/12/27.
//

#ifndef _RPC_SENT_CPP_
#define _RPC_SENT_CPP_

//#include "thread"
//#include "global.h"
#include "rpc_sent.h"
#include <arpa/inet.h>

#define BUFFER_SIZE 1024


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
void SendPlan(vector<Operator> plan, int target_site_id, int sourceId)
{
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
        cout <<  "SendResults :\t" << results <<endl;
        if(i != plan.size()-1)
            results.append("$");
    }
    //call
    string target_site_ip=mapIdtoIp(target_site_id, sourceId);
    socket_client(target_site_ip,results);
}

void socket_client(string target_site_ip,string results)
{
    cout << "socket_client"<<endl;
    char **argv;
    argv = new char*[1];
    argv[0] = new char[255];
    strcpy(argv[0],target_site_ip.c_str());
    cout << "target_site_ip.c_str()"<< argv[0]<<endl;
    // 设置一个socket地址结构client_addr, 代表客户机的internet地址和端口
    struct sockaddr_in client_addr;
    bzero(&client_addr, sizeof(client_addr));
    client_addr.sin_family = AF_INET; // internet协议族
    client_addr.sin_addr.s_addr = htons(INADDR_ANY); // INADDR_ANY表示自动获取本机地址
    client_addr.sin_port = htons(0);

    // 创建用于internet的流协议(TCP)类型socket，用client_socket代表客户端socket
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0)
    {
        printf("Create Socket Failed!\n");
        exit(1);
    }
    // 把客户端的socket和客户端的socket地址结构绑定
    if (bind(client_socket,(struct sockaddr*)&client_addr, sizeof(client_addr)))
    {
        printf("Client Bind Port Failed!\n");
        exit(1);
    }
    // 设置一个socket地址结构server_addr,代表服务器的internet地址和端口
    struct sockaddr_in  server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    // 服务器的IP地址来自程序的参数
    if (inet_aton(argv[0], &server_addr.sin_addr) == 0)
    {
        printf("Server IP Address Error!\n");
        exit(1);
    }
    server_addr.sin_port = htons(SERVER_PORT);
    socklen_t server_addr_length = sizeof(server_addr);
    // 向服务器发起连接请求，连接成功后client_socket代表客户端和服务器端的一个socket连接
    if (connect(client_socket, (struct sockaddr*)&server_addr, server_addr_length) < 0)
    {
        printf("Can Not Connect To %s!\n", argv[0]);
        exit(1);
    }
    char buffer[BUFFER_SIZE];
    bzero(buffer, sizeof(buffer));
    strncpy(buffer, results.c_str(), results.length()  > BUFFER_SIZE ? BUFFER_SIZE : results.length());
    send(client_socket, buffer, BUFFER_SIZE, 0);
    // 关闭与服务器端的连接
    close(client_socket);
}
void SendTable(int frag_id, string frag_content, string origin_table_name, int target_site_id, int sourceId)
{
    cout <<  "start SendTable \t" << endl;
    string target_site_ip=mapIdtoIp(target_site_id, sourceId);
    string frag_content_and_frag_id;
    string str=to_string(frag_id);
    frag_content_and_frag_id='1'+str+'#'+frag_content +'#' + origin_table_name;
    cout <<  "frag_content_and_frag_id\t" << frag_content_and_frag_id << endl;
    socket_client(target_site_ip,frag_content_and_frag_id);
}
void SendResultTable(int frag_id, string frag_content, string origin_table_name,  int target_site_id, int sourceId)
{
    string target_site_ip=mapIdtoIp(target_site_id, sourceId);
    string frag_content_and_frag_id;
    string str=to_string(frag_id);
    frag_content_and_frag_id='2'+str+'#'+frag_content +'#' +origin_table_name ;
    cout <<  "frag_content_and_frag_id\t" << frag_content_and_frag_id << endl;
    socket_client(target_site_ip,frag_content_and_frag_id);
/*
    rpc::client client(target_site_ip,target_port);
    bool ok = client.call("excute_result_table",frag_id, frag_content).as<bool>();
    cout <<  "return ok\t" << ok  << endl;
    return ok;*/
}


#endif