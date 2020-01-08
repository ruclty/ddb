//
// Created by dyj on 2019/12/8.
//
#include "rpc_receive.h"
//#include "../querytree/querytree.h"
#include "../global.h"
#include  <map>
#include <pthread.h>
using namespace std;


//int site_id = 1;

rpc_receive::rpc_receive(int site_id){
    this->site_id = site_id;
    this->site_exc = site_excution(site_id);
}
rpc_receive::~rpc_receive(){}

//vector<string> split(string& src,const string& separator){
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
//        }
//     }while(index != string::npos);
//
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

void* Data_handle(void * sock_fd, rpc_receive rpc_rec) {
    int new_server_socket = *((int *) sock_fd);
    char buffer[BUFFER_SIZE];
    bzero(buffer, sizeof(buffer));
    length = recv(new_server_socket, buffer, BUFFER_SIZE, 0);
    if (length < 0) {
        printf("Server Recieve Operation Failed!\n");
    }
/*    char operation[OPERATION_NAME_MAX_SIZE + 1];
    bzero(operation, sizeof(operation));
    strncpy(operation, buffer,
            strlen(buffer) > OPERATION_NAME_MAX_SIZE ? OPERATION_NAME_MAX_SIZE : strlen(buffer));
    cout << "Data_handle operation\t" << operation << endl;*/
    if(buffer[0]=='0')
    {
        //string plans;
        //strncpy(plans,operation+1,OPERATION_NAME_MAX_SIZE+1);
        string temp = buffer;
        string plans(temp,1,BUFFER_SIZE-1);
        //string plans = operation;
        cout << "Data_handle plans\t" << plans << endl;
        rpc_rec.ReceivePlan(plans);
    }
    else if(buffer[0]=='1')
    {
        string temp = buffer;
        string sfrag_id(temp,1,1);
        int frag_id=atoi(sfrag_id.c_str());
        string frag_content(temp,3,BUFFER_SIZE-3);
        vector<string> frag_content_origin_table_name;
        frag_content_origin_table_name=split(frag_content,"#");
        frag_content=frag_content_origin_table_name[0];
        string origin_table_name= frag_content_origin_table_name[1];
        //cout << "Data_handle\t" << plans << endl;
        rpc_rec.ReceiveTable(frag_id,frag_content);
    }
    else if(buffer[0]=='2')
    {
        tring temp = buffer;
        string sfrag_id(temp,1,1);
        int frag_id=atoi(sfrag_id.c_str());
        string frag_content(temp,3,BUFFER_SIZE-3);
        vector<string> frag_content_origin_table_name;
        frag_content_origin_table_name=split(frag_content,"#");
        frag_content=frag_content_origin_table_name[0];
        string origin_table_name= frag_content_origin_table_name[1];
        
        rpc_rec.ReceiveResultTable(frag_id,frag_content,origin_table_name);
        //cout << "Data_handle\t" << plans << endl;
        //rpc_rec.excute_result_table(frag_id,frag_content);
    }
}

bool rpc_receive::ReceivePlan(string plans)
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
        std::cout <<  "plan.ope\t" << plan.ope  << endl;
        plan.result_frag_id = std::stoi(vecplans[2]);
        std::cout <<  "plan.result_frag_id\t" << plan.result_frag_id  << endl;
        plan.target_site_id = std::stoi(vecplans[3]);
        plan.is_end = std::stoi(vecplans[4]);
        
        for(int j=4;j<vecplans.size();j++)
            plan.table_names.push_back(vecplans[j]);

        results.push_back(plan);
        vector<Operator>::iterator it = results.begin();
        //std::cout << (*it).table_names <<endl;
    }
    this->received_plan = results;
    //std::cout<< "rpc_receive::received_plan[0].table_names\t" << this->received_plan[0].table_names << endl;
    return true;
}


bool rpc_receive::ReceiveTable(int frag_id, string frag_content)
{
    std::cout <<  "have received frag_id \t" << frag_id  << endl;
    std::cout <<  "have received frag_content\t" << frag_content  << endl;
    this->received_frag_id = frag_id;
    this->received_frag_content = frag_content;
//    string table_name = this->site_exc.mysql.gdd.get_frag_info(frag_id).table_name;
//    excute_rec_table(table_name, frag_content);
    return true;
}
void rpc_receive::ReceiveResultTable(int frag_id, string frag_content, string origin_table_name)
{
    std::cout <<  "have received frag_id \t" << frag_id  << endl;
    std::cout <<  "have received frag_content\t" << frag_content  << endl;
 //   this->received_frag_id = frag_id;
 //   this->received_frag_content = frag_content;
    this->site_exc->recieve_result_table(frag_id, frag_content, origin_table_name);
    //this->site_exc->excute_results(can_do);
//    string table_name = this->site_exc.mysql.gdd.get_frag_info(frag_id).table_name;
//    excute_rec_table(table_name, frag_content);
   // return true;
}
void startListening(int port){
    // set socket's address information
    // 声明并初始化一个服务器端的socket地址结构
    // 设置一个socket地址结构server_addr,代表服务器internet的地址和端口
	struct sockaddr_in client_addr;
	socklen_t length = sizeof(client_addr);

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htons(INADDR_ANY);
    server_addr.sin_port = htons(port);

    // create a stream socket
    // 创建socket，若成功，返回socket描述符
    // 创建用于internet的流协议(TCP)socket，用server_socket代表服务器向客户端提供服务的接口
    int server_socket = socket(PF_INET, SOCK_STREAM, 0);
    if (server_socket < 0)
    {
        printf("Create Socket Failed!\n");
        exit(1);
    }
    // 把socket和socket地址结构绑定
    // 绑定socket和socket地址结构
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)))
    {
        printf("Server Bind Port: %d Failed!\n", port);
        exit(1);
    }
    // server_socket用于监听
    if (listen(server_socket, 20))
    {
        printf("Server Listen Failed!\n");
        exit(1);
    }
    while(1)
    {
        printf("waiting for new connection...\n");
        pthread_t thread_id;
        int new_server_socket = accept(server_socket, (struct sockaddr*)&client_addr, &length);
        if (new_server_socket < 0)
        {
            printf("Server Accept Failed!\n");
            continue;
        }
        printf("A new connection occurs!\n");
        if (pthread_create(&thread_id,NULL,&Data_handle,(void *)(&new_server_socket)) == -1)  //创建一个线程
        {
            fprintf(stderr,"pthread_create error!\n");
            break;                                  //结束循环
        }
    }

    // 关闭监听用的socket
    close(server_socket);
}

