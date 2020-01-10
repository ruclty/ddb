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
#include <pthread.h>

using namespace std;

static void *get_plan(void* arg){    
  //  string sql = "select student.id,student.name,exam.mark,course.name from student,exam,course where student.id=exam.student_id and exam.course_id=course.id and student.id>1060000 and course.location!='CB-3'";
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
	}

static void *listening(void* arg){
	rpc_receive rec = rpc_receive(5);
    	startListening(8888, &rec);
	}

int main(){
	pthread_t thread[2];
     int no = 0, res;
     void * thrd_ret;
//     srand(time(NULL));    
//     for (no = 0; no < THREAD_NUMBER; no++) {
//          /* 创建多线程 */
//          res = pthread_create(&thread[no], NULL, thrd_func, (void*)no);
//          if (res != 0) {
//               printf("Create thread %d failed\n", no);
//               exit(res);
//          }
//     }
     pthread_create(&thread[0], NULL, &get_plan,(void*)no);
	pthread_create(&thread[0], NULL, &listening,(void*)no);
  //   printf("Create treads success\n Waiting for threads to finish...\n");
    // rpc_receive rec = rpc_receive(5);
    //	startListening(8888, &rec);
     for (no = 0; no < 1; no++) {
          //等待线程结束 
          res = pthread_join(thread[no], &thrd_ret);
          if (!res) {
            printf("Thread %d joined\n", no);
          } else {
            printf("Thread %d join failed\n", no);
          }
     }
  //   return 0;        



  //  MySql mysql(2);
//    mysql.load_datafile("./data/course.tsv")
    return 0;
}
