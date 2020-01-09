//
// Created by dyj on 2019/12/8.
//

#ifndef MYSERVER_LOCAL_SQL_H
#define MYSERVER_LOCAL_SQL_H

#include <string>
#include <vector>
#include <iostream>
#include "../global.h"
#include <iostream>
#include "rpc/server.h"
#include "../excute/site_excution.h"
using std::string;
using std::cout;
using std::endl;
using std::string;
using std::ofstream;
using std::vector;

void excute_rec_table(string table_name, string table_content,site_excution* site_exc);
void excute_result_table(string table_name, string table_content,site_excution* site_exc);
bool ReceivePlan(string,site_excution*);
bool ReceiveTable(int , string , site_excution*);
bool ReceiveResult(int , string , site_excution*);
void startListening(int );

// class rpc_receive{
// private:
//     int site_id = 2;
//     int received_frag_id;
//     string received_frag_content;
//     vector<Operator> received_plan;
//     site_excution site_exc = site_excution(site_id);
// public:

//     rpc_receive(int);
//     ~rpc_receive();
    
//     //bool ReceivePlan(string plans);
//     //bool ReceiveTable(int frag_id, string frag_content);
    
//     //void excute_rec_plan(vector<Operator> plans);
    
// };





#endif //MYSERVER_LOCAL_SQL_H
