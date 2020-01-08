#include "local_sql_excution.h"
#include "local_sql_excution.cpp"
//#include "local_sql.cpp"
#include <iostream>
using namespace std;
int main(){
	MySql *mysql = new MySql("site1",1);
    string SQL = "select * from frag_1 where name = 'I. Bates'";
   //int r = mysql->excute_select_sql(SQL, 6);
   //string r  = mysql->select_all_table("temp_table_6");
   	//mysql->create_received_table(r,3,"frag_1");
    mysql->release_all_temp_table();
	// cout << r << endl;
	return 0;
}
