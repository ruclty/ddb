#ifndef _LOCAL_SQL_EXCUTION_CPP_
#define _LOCAL_SQL_EXCUTION_CPP_

//
//  local_sql_excution.cpp
//  
//
//  Created by baiping on 2019/12/20.
//
//#include "../gdd/gdd.h"
#include "local_sql_excution.h"
#include "../gdd/gdd.cpp"
using namespace std;

MySql::MySql(string ddb_name, int site_id){
    this->db_name = ddb_name;
    this->site_id = site_id;
    //generate_gdd();
}
MySql::~MySql(){}
//
//vector<string> MySql::split(const string& str, const string& delim) {  
//    vector<string> res;  
//    if("" == str) return res;  
//    //先将要切割的字符串从string类型转换为char*类型  
//    char * strs = new char[str.length() + 1] ; //不要忘了  
//    strcpy(strs, str.c_str());   
// 
//    char * d = new char[delim.length() + 1];  
//    strcpy(d, delim.c_str());  
// 
//    char *p = strtok(strs, d);  
//    while(p) {  
//        string s = p; //分割得到的字符串转换为string类型  
//        res.push_back(s); //存入结果数组  
//        p = strtok(NULL, d);  
//    }  
// 
//    return res;  
//}



frag_info MySql::load_new_frag_info(int frag_id,string frag_name,vector<string> attr_names,int size,int site_id){
    frag_info new_frag = frag_info();
    new_frag.frag_id = frag_id;
   // new_frag.table_name = frag_name;
    new_frag.attr_names = attr_names;
    //new_frag.attributes = attributes;
    new_frag.size = size;
    new_frag.site_id = site_id;
    return new_frag;
}

int MySql::excute_select_sql(string Operator, int result_frag_id){
    string temp_table_name = "temp_table_" + to_string(result_frag_id);

    vector<string> column;
    vector<attr_info> column_attr;
    vector<string> column_type;
    map<int,attr_info> attributes;
    bool is_union = false;
    int index_union = Operator.find("union");
	if(index_union > 0){
		is_union = true;
		string origin_table_name = Operator.substr(14,index_union-15);
		cout << "origin_table_namee:union " << origin_table_name << endl;
		attributes = get_frag_info(from_name_find_frag_id(origin_table_name)).attr_infos;
	}
    try{
    	MySQL_Driver *driver;
	    Connection *con;
        Statement *stmt;
        ResultSet *res_set;
        sql::ConnectOptionsMap connection_properties;
        connection_properties["hostName"] = this->host;
        connection_properties["userName"] = this->user_name;
        connection_properties["password"] = this->passwd;
        connection_properties["schema"] = this->db_name;
        connection_properties["port"] = this->port;
        connection_properties["CLIENT_LOCAL_FILES"] = true;
        driver = sql::mysql::get_driver_instance();
        con = driver->connect(connection_properties);
        stmt = con->createStatement();
        res_set = stmt->executeQuery(Operator);
        
        int col_cnt = res_set->getMetaData()->getColumnCount();
        string sql = "create table " + temp_table_name + " (";
 
        map<string,bool> col_name;
       // map<int,bool> delet_col;
        vector<int> delet_col_id;
	   for(int i = 1; i<= col_cnt; i++){
            cout << sql << endl;
            int type_int = res_set->getMetaData()->getColumnType(i);
            //cout << "attr_type:" <<  to_string(type_int) << endl;
            string type;
            if(type_int == 5)
	   		  type = "int";
            else if(type_int == 13)
              type = "varchar(25)";
            else if(type_int == 11)
              type = "varchar(1)";


            string attr_name = res_set->getMetaData()->getColumnName(i);
        
            cout << "attr_name:" << attr_name << endl;
		  column_type.push_back(type);
		  attr_info tm_attr_info;
		  tm_attr_info.type = type;
		  tm_attr_info.attr_name = attr_name;
		  if(is_union == false)
		  	attributes[i] = tm_attr_info;	

		/////////////////////////////////////////save gdd information
		  if(col_name[attributes[i].attr_name] == false){
		  		col_name[attributes[i].attr_name] = true;
		  		//delet_col[i] = false;

		  		column.push_back(attributes[i].attr_name);
		  //cout << attr_name << endl;
	   			string col_name = attributes[i].attr_name;
        			string col_type = type;
        			string t = col_name + " " + col_type;
        			sql += t;

        			if(i != col_cnt)
        				sql += ',';
        			else
        				sql += ')';
		  	}

		  else{
		  		delet_col_id.push_back(i);
		  	}
            //print_attr_info(attributes[attributes.size()-1]);
            
	   	}
	 
        cout <<"sql is:" + sql << endl;
        
        stmt->executeUpdate(sql);
        //cout << "Is dead" << endl;


        int line_count = 0;
        while(res_set->next()){
        	string sql = "insert into " + temp_table_name + " values (";

        	for(int colIndex = 1; colIndex <= col_cnt; colIndex ++){
        			cout << sql << endl;
        			bool is_delete = false;
        			for(auto del_index : delet_col_id ){
        				if(del_index == colIndex)
        					is_delete = true;
        					cout << "delete:" << colIndex << endl;
        				}

        			string temp = "";
 				string col_type = column_type[colIndex-1];
 				if(col_type[0] == 'i')
 					temp += res_set->getString(colIndex);
 				else{
 					string t = "'" + res_set->getString(colIndex) + "'";
 					temp += t;
 				}
				if(is_delete == true)
        				continue;
 				sql += temp;
 				if(colIndex != col_cnt)
 					sql += ",";
 				else
 					sql += ")";
               // cout << sql << endl;
        	}
          cout << sql << endl;
        	stmt->executeUpdate(sql);
        	line_count += 1;
        }
        cout << "Is dead" << endl;
        //res.pop_back();
    
        // cout << res << endl;
        con->close();    
        delete res_set;
        delete stmt;
        delete con;

         update_frag_info(result_frag_id,column,attributes,line_count);
         cout << "select execution finished!" << endl; 
    }catch(sql::SQLException &e){
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line "
        << __LINE__ << endl;
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }
    return result_frag_id;
}

string MySql::select_all_table(string table_name){
	string res = " ";
	string Operator = "select * from "+ table_name;
	try{
    	MySQL_Driver *driver;
        Connection *con;
        Statement *stmt;
        ResultSet *res_set;
        sql::ConnectOptionsMap connection_properties;
        connection_properties["hostName"] = this->host;
        connection_properties["userName"] = this->user_name;
        connection_properties["password"] = this->passwd;
        connection_properties["schema"] = this->db_name;
        connection_properties["port"] = this->port;
        connection_properties["CLIENT_LOCAL_FILES"] = true;
        driver = sql::mysql::get_driver_instance();
     //   cout << driver << endl;
      //  cout << "Is ee6" << endl;
        // con = driver->connect(getMysqlIp(),getUsername(),getPassword());
        con = driver->connect(connection_properties);
      //  cout << "Is ee7" << endl;
        //con->setSchema(this->db_name);
       // cout << "Is ee8" << endl;
        stmt = con->createStatement();

        res_set = stmt->executeQuery(Operator);
        //cout << db_stmt << endl;
        int col_cnt = res_set->getMetaData()->getColumnCount();

        int line_count = 0;
        while(res_set->next()){
           for(int i = 1;i <= col_cnt;i++){
                if(5 == res_set->getMetaData()->getColumnType(i)){
                    res.append(res_set->getString(i));
                }
                else{
                    string t = "'" + res_set->getString(i) + "'";
                    res.append(t);
                }
                if(i != col_cnt)
                    res.append(",");
           }
            res.append("\n");
        	}
        
        res.pop_back();
    
        // cout << res << endl;
        con->close();    
        delete res_set;
        delete stmt;
        delete con;
    }catch(sql::SQLException &e){
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line "
        << __LINE__ << endl;
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }
    return res;
}

vector<string> MySql::get_table_names(){
    vector<string> tables;
    try{
        string SQL = "show tables;";

        
        MySQL_Driver *driver;
        Connection *con;
        Statement *stmt;
        ResultSet *res_set;
        sql::ConnectOptionsMap connection_properties;
        connection_properties["hostName"] = this->host;
        connection_properties["userName"] = this->user_name;
        connection_properties["password"] = this->passwd;
        connection_properties["schema"] = this->db_name;
        connection_properties["port"] = this->port;
        connection_properties["CLIENT_LOCAL_FILES"] = true;
        driver = sql::mysql::get_driver_instance();
        con = driver->connect(connection_properties);
        stmt = con->createStatement();


        res_set = stmt->executeQuery(SQL);
        int c = 0;
        int col_cnt = res_set->getMetaData()->getColumnCount();
        while(res_set->next()){
            for(int i = 1;i <= col_cnt;i++){
                    tables.push_back(res_set->getString(i));
                    cout << "xxxxxxdddddd" << endl;
             
           }
        }
        con->close();    
        delete res_set;
        delete stmt;
        delete con;


    }catch(sql::SQLException &e){
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line "
        << __LINE__ << endl;
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }
    return tables;
}
void MySql::release_all_temp_table(){
	try{
        string SQL = "show tables;";
        vector<string> to_release_table;
        
		MySQL_Driver *driver;
        Connection *con;
        Statement *stmt;
        ResultSet *res_set;
        sql::ConnectOptionsMap connection_properties;
        connection_properties["hostName"] = this->host;
        connection_properties["userName"] = this->user_name;
        connection_properties["password"] = this->passwd;
        connection_properties["schema"] = this->db_name;
        connection_properties["port"] = this->port;
        connection_properties["CLIENT_LOCAL_FILES"] = true;
        driver = sql::mysql::get_driver_instance();
        con = driver->connect(connection_properties);
        stmt = con->createStatement();


        res_set = stmt->executeQuery(SQL);
        int c = 0;
        int col_cnt = res_set->getMetaData()->getColumnCount();
        while(res_set->next()){
            for(int i = 1;i <= col_cnt;i++){
                    //cout << result->getMetaData()->getColumnType(i) << endl;
                    //cout << (result->getString(i)) << endl;
                    to_release_table.push_back(res_set->getString(i));
                  //  cout << "xxxxxxdddddd" << endl;
             
           }
        }

        for(int i=0;i<to_release_table.size();i++){
        	string table_name = to_release_table[i];
        	if(table_name.find("temp") == -1)
        		continue;//if the table is not temp table ,continue
        	else{// if the table is the temp table ,release it;
        		string sql = "drop table " + table_name + ";";
        		stmt->executeUpdate(sql);
        //		drop_frag_info("dTB",table_name);
        	}
        }
        con->close();    
        delete res_set;
        delete stmt;
        delete con;


	}catch(sql::SQLException &e){
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line "
        << __LINE__ << endl;
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }

}

void MySql::create_received_table(string table_content, int result_frag_id, string origing_table_name,bool is_result){
	string temp_table_name;
    if(is_result){
        vector<string> table_names = this->get_table_names();
        int res_id = 0;
        for(int i=0; i< table_names.size();i++)
            if(table_names[i][0] == 'r')
                res_id+=1;
        temp_table_name = "result_table_"+ to_string(res_id);
    }
    else
        temp_table_name = "temp_table_" + to_string(result_frag_id);

    int origin_frag_id = from_name_find_frag_id(origing_table_name);
	cout << origing_table_name << endl;
   // vector<attr_info> table_attributes;
	vector<string> table_attr_name;
	map<int,attr_info> attributes;

    frag_info origin_frag_info = get_frag_info(origin_frag_id);
    table_info origin_table_info = get_table_info(origin_frag_info.table_name);
    
    if(origin_frag_info.is_temp){
    		 table_attr_name = get_frag_info(origin_frag_id).attr_names;
    		 attributes = get_frag_info(origin_frag_id).attr_infos;
    }
    else{
    		cout << "fsfsfdsfs" << endl;
		 table_attr_name = origin_table_info.attr_names;
		 for(int i=0;i<table_attr_name.size();i++){
		 	attr_info tm_attr_info = get_attr_info(origing_table_name,table_attr_name[0]);
		 	attributes[i+1]= tm_attr_info;
		 	}
    	}
    	cout << "table_attr_mame_size:" << table_attr_name.size() << endl;
    	for(auto t :table_attr_name)
    		print_attr_info(get_attr_info(origin_frag_info.table_name,t));
//    for(int i = 0; i< table_attr_name.size(); i++){
//        attr_info table_att = get_attr_info(origing_table_name, table_attr_name[i]);
//        table_attributes.push_back(table_att);
//    }
    vector<string> insert_content = split(table_content,"\n");
    vector<string> column = table_attr_name;

//    vector<attr_info> column_attr = table_attributes;
    try{
       MySQL_Driver *driver;
        Connection *con;
        Statement *stmt;
        ResultSet *res_set;
        sql::ConnectOptionsMap connection_properties;
        connection_properties["hostName"] = this->host;
        connection_properties["userName"] = this->user_name;
        connection_properties["password"] = this->passwd;
        connection_properties["schema"] = this->db_name;
        connection_properties["port"] = this->port;
        connection_properties["CLIENT_LOCAL_FILES"] = true;
        driver = sql::mysql::get_driver_instance();
     //   cout << driver << endl;
      //  cout << "Is ee6" << endl;
        // con = driver->connect(getMysqlIp(),getUsername(),getPassword());
        con = driver->connect(connection_properties);
      //  cout << "Is ee7" << endl;
        //con->setSchema(this->db_name);
       // cout << "Is ee8" << endl;
        stmt = con->createStatement();

        //int col_cnt = (split(insert_content[0],",")).size();
        string sql = "create table " + temp_table_name + " (";
        cout << to_string(column.size()) << endl;
        vector<string> at_names;
       for(int i = 1; i<= column.size(); i++){
            //cout << sql << endl;
            // int type = res_set->getMetaData()->getColumnType(i);
            // //cout << to_string(type) << endl;
            // if(type == 5)
            //   ai.type = "int";
            // if(type == 13)
            //   ai.type = "varchar(25)";
            // if(type == 11)
            //   ai.type = "varchar(1)";

            //cout << ai.type << endl;
            //cout << "ddddd" << endl;
            // ai.attr_name = res_set->getMetaData()->getColumnName(i);
            // //cout << "vvvvvv" << endl;
            // column.push_back(ai.attr_name);
            // column_attr.push_back(ai);
          //  attr_info temp_info = get_attr_info(origin_frag_info.table_name,table_attr_name[i-1]);
            string col_name = attributes[i].attr_name;
            string col_type = attributes[i].type;
          //  print_attr_info(attributes[i]);
            at_names.push_back(col_name);
            string t = col_name + " " + col_type;
            sql += t;

            if(i != column.size())
                    sql += ',';
            else
                    sql += ')';
        }
     
        cout <<"sql is:" + sql << endl;
        
        stmt->executeUpdate(sql);

        
		cout << "is created??????????????" << endl;
        int line_count = 0;
        for(int i = 0;i < insert_content.size();i++){
            string sql = "insert into " + temp_table_name + " values (";
            sql += insert_content[i];
            sql += ")";
            cout << sql << endl;
            stmt->executeUpdate(sql);
            line_count += 1;
        }
    
        // cout << res << endl;
        con->close();    
        delete res_set;
        delete stmt;
        delete con;
        update_frag_info(result_frag_id,at_names,attributes,line_count);
        // frag_info new_frag = frag_info();
        // new_frag.frag_id = result_frag_id;
        // new_frag.table_name = temp_table_name;
        // new_frag.attributes = table_attributes;
        // new_frag.size = line_count;
        // new_frag.site_id = this->site_id;
        // save_frag_info(new_frag);
    }catch(sql::SQLException &e){
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line "
        << __LINE__ << endl;
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    }  

}

//int MySql::create_allocated_table(string table_content, int frag_id,string table_name){
//    int temp_id = get_new_frag_id();
//    string temp_table_name = table_name;
//
//    frag_info source_table_info = get_frag_info(frag_id);
//    vector<attr_info> table_attributes = source_table_info.attributes;
//
//    vector<string> insert_content = this->split(table_content,"\n");
//
//    try{
//        MySQL_Driver *driver;
//        Connection *con;
//        Statement *stmt;
//        ResultSet *res_set;
//        sql::ConnectOptionsMap connection_properties;
//        connection_properties["hostName"] = this->host+":"+to_string(this->port);
//        connection_properties["userName"] = this->user_name;
//        connection_properties["password"] = this->passwd;
//        connection_properties["port"] = this->port;
//        connection_properties["CLIENT_LOCAL_FILES"] = true;
//        driver = sql::mysql::get_driver_instance();
//        // con = driver->connect(getMysqlIp(),getUsername(),getPassword());
//        con = driver->connect(connection_properties);
//        con->setSchema(this->db_name);
//        stmt = con->createStatement();
//
//        string sql = "create table " + temp_table_name + " (";
//        for(int colIndex = 0; colIndex < table_attributes.size(); colIndex ++){
//            attr_info col_attribute_info = table_attributes[colIndex];
//            string col_name = col_attribute_info.type;
//            string col_type = col_attribute_info.type;
//            string t = col_name + " " + col_type;
//            sql += t;
//            if(colIndex != table_attributes.size())
//                sql += ',';
//            else
//                sql += ')';
//        }
//        stmt->executeUpdate(sql);
//
//        
//
//        int line_count = 0;
//        for(int i = 0;i < insert_content.size();i++){
//            string sql = "insert into " + temp_table_name + " values (";
//            sql += insert_content[i];
//            sql += ")";
//            
//            stmt->executeUpdate(sql);
//            line_count += 1;
//        }
//    
//        // cout << res << endl;
//        con->close();    
//        delete res_set;
//        delete stmt;
//        delete con;
//
//        frag_info new_frag = frag_info();
//        new_frag.frag_id = temp_id;
//        new_frag.table_name = temp_table_name;
//        new_frag.attributes = table_attributes;
//        new_frag.size = line_count;
//        new_frag.site_id = this->site_id;
//        save_frag_info(new_frag);
//    }catch(sql::SQLException &e){
//        cout << "# ERR: SQLException in " << __FILE__;
//        cout << "(" << __FUNCTION__ << ") on line "
//        << __LINE__ << endl;
//        cout << "# ERR: " << e.what();
//        cout << " (MySQL error code: " << e.getErrorCode();
//        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
//    }
//    return temp_id;
//    
//
//}
//
//int MySql::load_datafile(string table_name, vector<string> attr_names, vector<string> attr_types, string key, string sql_file){
//    //string sql_file;
//    // if(table_name == "Student")
//    //     sql_file "./data/student.tsv";
//    // if(table_name == "Course")
//    //     sql_file "./data/course.tsv";
//    // if(table_name == "Teacher")
//    //     sql_file "./data/teacher.tsv";
//    // if(table_name == "Exam")
//    //     sql_file "./data/exam.tsv";
//
//    vector<attr_info> attr_infos;
//    for(int i = 0; i < attr_names.size();i++){
//        attr_info new_attr = attr_info();
//        new_attr.attr_name = attr_names[i];
//        new_attr.type = attr_types[i];
//        if(attr_names[i] == key)
//            new_attr.is_key == 1;
//        attr_infos.push_back(new_attr);
//    }
//    string db_stmt = "";
//    int frag_id;
//    try{
//        MySQL_Driver *driver;
//        Connection *con;
//        Statement *stmt;
//        ResultSet *res_set;
//        bool res = false;
//        sql::ConnectOptionsMap connection_properties;
//        connection_properties["hostName"] = "jdbc:mysql://localhost/";
//        connection_properties["userName"] = this->user_name;
//        connection_properties["password"] = this->passwd;
//        connection_properties["port"] = this->port;
//        connection_properties["CLIENT_LOCAL_FILES"] = true;
//
//        driver = sql::mysql::get_mysql_driver_instance();
//        con = driver->connect(connection_properties);
//        // con = driver->connect(getMysqlIp(),getUsername(),getPassword());
//        con->setSchema(this->db_name);
//        stmt = con->createStatement();
//        db_stmt = "load data infile '" 
//        + sql_file 
//        + "' into table " 
//        + table_name 
//        + " fields terminated by '\t' " 
//        + "lines teminated by '\n';";
//        cout << db_stmt << endl;
//        // string sql = "select * from customer";
//        stmt ->execute(db_stmt);
//        res = true;
//        cout << "load: " << res << endl;
//        con->close();
//        delete stmt;
//        delete con;
//
//        frag_id = get_new_frag_id();
//        string count = "select count(*) from " + table_name +';';
//        res_set = stmt->executeQuery(count);
//        int size = res_set->getInt(1);
//
//        vector<string> attr_names;
//        vector<attr_info> attributes;
//        frag_info new_frag_info = this->load_new_frag_info(frag_id,table_name,attr_names,attr_infos,size,this->site_id);
//
//        save_frag_info(new_frag_info);
//    } catch (sql::SQLException &e){
//        cout << "# ERR: SQLException in " << __FILE__;
//        // cout << "(" << __FUNCTION__ << ") on line "»
//        // << __LINE__ << endl;
//        cout << "# ERR: " << e.what();
//        cout << " (MySQL error code: " << e.getErrorCode();
//        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
//    }   
//    return frag_id;
//}

// map<string,int> MySQL::do_fragment(vector<string> Operator, int fragment_num,string table_name){
//     map<string,int> new_temp_id;
//     for(int i=0;i < fragment_num;i++){
//         int temp_id = this->excute_select_sql(Operator[i]);
//         string fragment_name = table_name+'.'+to_string(i);
//         new_temp_id[fragment_name] = temp_id;
//     }
//     return new_temp_id;
// }
#endif
