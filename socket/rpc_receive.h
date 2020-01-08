//
// Created by dyj on 2019/12/8.
//

#ifndef MYSERVER_LOCAL_SQL_H
#define MYSERVER_LOCAL_SQL_H

#include <string>
#include <vector>
#include <iostream>
#include "thread"
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include "../global.h"
// #include "../excute/site_excution.h"
using std::string;
using std::cout;
using std::endl;
using std::string;
using std::ofstream;
using std::vector;

class rpc_receive{

private:
    int site_id;
public:

    rpc_receive(int);
    ~rpc_receive();

    site_excution site_exc;
    bool ReceivePlan(string plans);
    bool ReceiveTable(int frag_id, string frag_content);
    bool ReceiveResultTable(int frag_id, string frag_content);
    //vector<string> split(string& src,const string& separator);
};

void startListening(int port);
static void* Data_handle(void * sock_fd);



#endif //MYSERVER_LOCAL_SQL_H
