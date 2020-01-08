//
// Created by dyj on 2019/12/27.
//

#ifndef MYSERVER_RPCCALL_H
#define MYSERVER_RPCCALL_H

#include <string>
#include <vector>
#include <iostream>
#include "../global.h"

using std::string;
using std::cout;
using std::endl;
using std::string;
using std::ofstream;
using std::vector;

bool SendResult(int frag_id, string frag_content, int target_site_id);
bool SendPlan(vector<Operator> plan, int target_site_id);
bool SendTable(int frag_id, string frag_content, int target_site_id);
string mapIdtoIp(int id);
#endif //MYSERVER_RPCCALL_H
