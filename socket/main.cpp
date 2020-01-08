#include <iostream>
#include "../global.h"
#include "rpc_receive.h"
#include "rpc_sent.h"
using namespace std;

int main() {
    //std::cout << "Hello, World!" << std::endl;

    string frag_content="frag_content";
    int target_site_id=1;
    int frag_id=2;

    vector<Operator> plan(1);
    plan[0].content = "select * from table";
    plan[0].ope = ESS;
    plan[0].result_frag_id = 3;
    plan[0].target_site_id = 4;
    plan[0].is_end = 0;
    vector<string> vectable_names = {"tb1","tb2"};
    plan[0].table_names=vectable_names;
    rpc_receive rpc_rec = rpc_receive(1);
    startListening(6666);// start listening at local

    //SendPlan(plan, target_site_id); // rpc to SendPlan
    //SendTable( frag_id, frag_content, target_site_id);  //rpc to SendTable
    //SendResult( frag_id, frag_content, target_site_id);  //rpc to SendTable
    return 0;
}
