//#include "excute/site_excution.cpp"
//#include "StartListening.h"
//#include "socket/rpc_sent.cpp"
#include "socket/rpc_receive.cpp"
#include "global.h"
using namespace std;


int main(){
    rpc_receive rec = rpc_receive(2);
<<<<<<< HEAD
    startListening(6666, &rec);
=======
    startListening(7777, &rec);
>>>>>>> 59d44a94deca7defae0f836f20b983581b51a13a
}
