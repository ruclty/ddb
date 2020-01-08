#include "local_sql.h"
//#include "local_sql.cpp"
#include <iostream>
using namespace std;
int main(){
	// MySql mysql = MySql("site1",1);
	char* SQL = "show tables;";
	// int result_frag_id = 8;
	// int temp_frag = mysql.excute_select_sql(sql, result_frag_id);
	// cout << to_string(temp_frag) << endl;
	//string m = localExecuteQuery("site1",sql);
	//cout << m << endl;

    //初始化驱动
    try
    {
    	MySQL_Driver *driver = NULL;
    	Connection *con = NULL;
    	ResultSet *result = NULL;
 		Statement *stmt = NULL;
 		//cout << stmt << endl;
 		cout << con << endl;
 		cout << result << endl;
    	cout << "is wrong?0" << endl;
        driver=(sql::mysql::get_mysql_driver_instance());
        cout << "is wrong?22" << endl;
        //建立连接
        sql::ConnectOptionsMap connection_properties;
        cout << "is wrong?  1" << endl;
        connection_properties["hostName"] = "tcp://127.0.0.1:3306";
        connection_properties["userName"] = "root";
        connection_properties["password"] = "ddb09890";
        connection_properties["schema"] = "site1";
        connection_properties["port"] = 3306;
        connection_properties["CLIENT_LOCAL_FILES"] = true;
        //con = driver->connect("tcp://127.0.0.1:3306","root","ddb09890");
        con = driver->connect(connection_properties);
        cout << con << endl;
        cout << driver << endl;
 		cout << "is wrong?12" << endl;
        if(con->isValid() == false){
        	cout<<"连接失败"<<endl;
        	return -1;
        	}
        cout << "is wrong?" << endl;
        //con->setSchema("test");
        cout << "dddd" << endl;
        stmt = con->createStatement();
        //stmt->execute("SET NAMES utf8");
        cout << "xsds" << endl;
        //stmt->execute("USE site1");
        cout << "is wrong?1" << endl;
        cout << "status: " << con->isClosed() << std::endl;
        //stmt = con->createStatement();

        //stmt->execute("use mysql");
        
        cout << stmt << endl;
        cout << "is wrong?2" << endl;
        result = stmt->executeQuery(SQL);
        int c = 0;
        int col_cnt = result->getMetaData()->getColumnCount();
        while(result->next()){
        	for(int i = 1;i <= col_cnt;i++){
        			cout << result->getMetaData()->getColumnType(i) << endl;
                    cout << (result->getString(i)) << endl;
                    cout << "xxxxxxdddddd" << endl;
             
           }
        }
        cout << "xxx" << endl;
 		}catch (exception &e){
 				cout << "catch:" << e.what() << endl;
		        // cout<<e.what()<<",state:"<<e.getSQLState()<<endl;
		        // cout<<"errorCode: " << e.getErrorCode()<<endl;
			}

	return 0;
}