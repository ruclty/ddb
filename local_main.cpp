//#include "excute/site_excution.cpp"
//#include "StartListening.h"
//#include "socket/rpc_sent.cpp"
#include "socket/rpc_receive.cpp"
#include "global.h"
using namespace std;


int main(){
    rpc_receive rec = rpc_receive(1);

    startListening(6666, &rec);

}
