#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include "querytree/querytree.cpp"
#include "gdd/gdd.cpp"
#include "excute/QueryOptimise.cpp"
#include "excute/QueryOptimise.h"
#include "excute/local_sql_excution.cpp"
#include "socket/rpc_receive.cpp"

using namespace std;

int main(){
    
//    string sql = "select student.id,student.name,exam.mark,course.name from student,exam,course where student.id=exam.student_id and exam.course_id=course.id and student.id>1060000 and course.location!='CB-3'";
	string sql = "select * from student where student.id<1000010";
    cout << sql << endl;
    //sql="select * from a where a.a!='a'";
    query_tree tree(sql);
    tree.parser_sql();
    tree.generate_tree();
    tree.print_tree();
    tree.frag_table();
    tree.print_tree();
    tree.push_select();
    tree.print_tree();
    tree.prune_join();
    tree.print_tree();
    query_tree_node* root = tree.get_root();
    query_plan plan(root);
    //MySql mysql(1);
    //mysql.load_datafile("./data/course.tsv","course");
    plan.excute_query_plan(root,0);

    rpc_receive rec = rpc_receive(5);
    startListening(8888, &rec);

  //  MySql mysql(2);
//    mysql.load_datafile("./data/course.tsv")
    return 0;
}
