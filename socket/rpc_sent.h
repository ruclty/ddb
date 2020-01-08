//
// Created by dyj on 2019/12/27.
//

#ifndef MYSERVER_RPCCALL_H
#define MYSERVER_RPCCALL_H

#include <string>
#include <vector>
#include <iostream>
#include "../global.h"

#include<stdio.h>
#include<stdlib.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<string.h>

#include <unistd.h>
#include <fcntl.h>

using std::string;
using std::cout;
using std::endl;
using std::string;
using std::ofstream;
using std::vector;

void SendResult(int frag_id, string frag_content, string origin_table_name,int target_site_id);
void SendPlan(vector<Operator> plan, int target_site_id);
void SendTable(int frag_id, string frag_content, string origin_table_name, int target_site_id);
void socket_client(string target_site_ip,string results);
//string mapIdtoIp(int id);
#endif //MYSERVER_RPCCALL_H
