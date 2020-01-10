#include "QueryOptimise.h"
#include "../gdd/gdd.h"
#include "../querytree/querytree.h"
#include <vector>
#include <map>
#include <algorithm>
using namespace std;



query_plan::query_plan(query_tree_node* root){
    this->root = root;
   // this->generate_gdd();
}

query_plan::~query_plan(){}

int query_plan::semi_join(int target_site, int frag_id1, int frag_id2){

}

void query_plan::transfer_plan(){
    for(int i=1;i<=4;i++){
    	
    	   cout << to_string(plan[i].size()) << endl;
    	   if(plan[i].size() > 0)
        		SendPlan(this->plan[i], i, 5);

        cout << "send end" << endl;
    }
}

int query_plan::transfer_sql(string sql, int source_site_id, int excute_site_id, vector<string> table_names){
    cout << "trangsferring "+ sql << endl;
    cout << " from sourceId:" +to_string(source_site_id) + " to targetId:"+to_string(excute_site_id);
    frag_info new_frag_info = frag_info();
    int new_frag_id = get_new_frag_id();

    string new_frag_name = "temp_table_"+ to_string(new_frag_id);
    new_frag_info.frag_id = new_frag_id;
   // new_frag_info.table_name = new_frag_name;
    new_frag_info.site_id = excute_site_id;
    cout << ", new_frag_info.site_id :" + to_string(new_frag_info.site_id);
    save_frag_info(new_frag_info);
    cout << ", and the temp_table_id is:" + to_string(new_frag_id) << endl; 

    Operator now_sql;
    now_sql.content = sql;
    now_sql.ope = ESS;
    now_sql.result_frag_id = new_frag_id;
    now_sql.table_names = table_names;
    now_sql.is_end = 0;
    this->plan[excute_site_id].push_back(now_sql);
    return new_frag_id;
}
int query_plan::transfer_table(string table_name, int source_site_id, int excute_site_id){
    cout << "trangsferring "+ table_name << endl;
    cout << " from sourceId:" +to_string(source_site_id) + " to targetId:"+to_string(excute_site_id) ;

    frag_info source_frag_info = get_frag_info(from_name_find_frag_id(table_name));

    frag_info new_frag_info = frag_info();
    int new_frag_id = get_new_frag_id();
    string new_frag_name = "temp_table_"+ to_string(new_frag_id);
    new_frag_info.frag_id = new_frag_id;
    new_frag_info.table_name = new_frag_name;
    new_frag_info.site_id = excute_site_id;
    cout << ", new_frag_info.site_id :" + to_string(new_frag_info.site_id);
    new_frag_info.size = source_frag_info.size;
    new_frag_info.attr_names = source_frag_info.attr_names;
    //new_frag_info.attributes = source_frag_info.attributes;
    save_frag_info(new_frag_info);
    cout << "and the temp_table_id is:" + to_string(new_frag_id) << endl; 

    Operator now_sql;
    now_sql.content = table_name;
    now_sql.ope = SAT;
    now_sql.result_frag_id = new_frag_id;
    now_sql.target_site_id = excute_site_id;
    now_sql.is_end = 0;
    this->plan[source_site_id].push_back(now_sql);

    // Operator now_sql;
    // now_sql.content = to_string(excute_site_id);
    // now_sql.ope = TRANS;
    // now_sql.result_frag_id = temp_frag_id;
    // this->plan[source_site_id].push_back(now_sql);

//    Operator now_sql1;
//    now_sql1.content = to_string(source_site_id);
//    now_sql1.ope = REC;
//    now_sql1.result_frag_id = new_frag_id;
//    this->plan[excute_site_id].push_back(now_sql1);
    return new_frag_id;
}

// transfer the sql from source_site_id to target_site_id; need some function from rpc
// void transfer_sql(stirng sql, int source_site_id, int target_site_id){

// }
// // transfer the table data from source_site_id to target_site_id; need some function from rpc
// void transfer_table(string table_name, int source_site_id, int target_site_id){

// }
// how to transfer to reduce the cost of transferring.
vector<frag_info> query_plan::how_to_transfer(vector<int> frag_ids){
    vector<frag_info> frag_infos;
    map<int,int> size_of;
    //cout << "frag_ids_size:" + to_string(frag_ids.size()) << endl;
    for(int fragIndex = 0; fragIndex < frag_ids.size(); fragIndex ++){
        frag_info temp = get_frag_info(frag_ids[fragIndex]);

        frag_infos.push_back(get_frag_info(frag_ids[fragIndex]));

        //cout << to_string(frag_infos[fragIndex].site_id) << "," << endl;
        if(frag_infos[fragIndex].site_id == 1)
            //size_of[1] += frag_infos[fragIndex].size;
            size_of[1] += 1;
        if(frag_infos[fragIndex].site_id == 2)
            size_of[2] += 1;
            //size_of[2] += frag_infos[fragIndex].size;
        if(frag_infos[fragIndex].site_id == 3)
            size_of[3] += 1;
            //size_of[3] += frag_infos[fragIndex].size;
        if(frag_infos[fragIndex].site_id == 4)
            size_of[4] += 1;
            //size_of[4] += frag_infos[fragIndex].size;

        cout <<"frag_site:"+ to_string(frag_infos[fragIndex].site_id) << endl;
    }

    int target_site_id = 0;
    cout << "1:" + to_string(size_of[1]) +", 2:" + to_string(size_of[2])+ ", 3:" + to_string(size_of[3])+ ", 4:" + to_string(size_of[4])<< endl;
    if (size_of[1] < size_of[2]){
        if(size_of[3] < size_of[2])
            target_site_id = 2;
        else
            target_site_id = 3;
    }
    else{
        if(size_of[3] < size_of[1])
            target_site_id = 1;
        else
            target_site_id = 3;
    }
    if(size_of[4] > size_of[target_site_id])
        target_site_id = 4;
    cout << "target_site_id:" + to_string(target_site_id) << endl;
    vector<frag_info> target_frag_infos;
    for(int fragIndex = 0;fragIndex < frag_ids.size(); fragIndex ++){
        frag_info now_frag_info = frag_infos[fragIndex];
        //cout << "now_frag_info_site_id:" + to_string(now_frag_info.site_id) << endl;
        if(now_frag_info.site_id == target_site_id){
            target_frag_infos.push_back(now_frag_info);
            continue;
        }

        int new_frag_id = transfer_table(get_frag_name(now_frag_info.frag_id),now_frag_info.site_id,target_site_id);
        frag_info new_frag_info = get_frag_info(new_frag_id);
        target_frag_infos.push_back(new_frag_info);
    }
    return target_frag_infos;
}

vector<frag_info> query_plan::how_transfer(vector<int> frag_ids){
    vector<frag_info> frag_infos;
    map<int,int> size_of;
    //cout << "frag_ids_size:" + to_string(frag_ids.size()) << endl;

    int target_site_id = 1;
    cout << "target_site_id:" + to_string(target_site_id) << endl;
    vector<frag_info> target_frag_infos;
    for(int fragIndex = 0;fragIndex < frag_ids.size(); fragIndex ++){
        frag_info now_frag_info = frag_infos[fragIndex];
        //cout << "now_frag_info_site_id:" + to_string(now_frag_info.site_id) << endl;
        if(now_frag_info.site_id == target_site_id){
            target_frag_infos.push_back(now_frag_info);
            continue;
        }

        int new_frag_id = transfer_table(get_frag_name(now_frag_info.frag_id),now_frag_info.site_id,target_site_id);
        frag_info new_frag_info = get_frag_info(new_frag_id);
        target_frag_infos.push_back(new_frag_info);
    }
    return target_frag_infos;
}


void query_plan::excute_one_operator(query_tree_node* node, int child_id)
{
	NODETYPE node_type = node->node_type;	
    cout << "node_type:" + node->get_str() << endl;
	// select: selectNode to sql,transfer,
	// select * 
	// from table_name(find_table_name from frag_id) 
	// where table_namFe.attribute = ".." or num.

	// child_num = 1
	// attribute_num = 1
	if(node_type == SELECT){
		// get sql
        vector<string> table_names;
		string result_sql = "select * from ";
		query_tree_node* frag_node = node->child[0];
        
		frag_info frag_information = get_frag_info(frag_node->frag_id);
        
		string table_name = get_frag_name(frag_information.frag_id);
        table_names.push_back(table_name);
        //cout << "table_name in sql:" + table_name << endl;
		result_sql += table_name;
        // select * from table_name;
		string predicate_string = " where ";
		predicate_string += table_name;
		predicate_string += '.';

        //cout << "predicate_string:" << predicate_string << endl;
        //cout << "pred_type:"+ptype_to_token(node->pred_type) << endl;
		// select * from table_name where table_name.
		if(node->pred_type == INT){
                //cout << "Is there wrong? 1" << endl;
				predicate_string += node->predv[0].attr_name;
              //  cout << "Is there wrong? 2" << endl; 
				predicate_string += rela_to_token(node->predv[0].rel);
            //    cout << "Is there wrong? 3" << endl;
				predicate_string += to_string(node->predv[0].value);
                //cout << "Is there wrong? 4" << endl;
            }
		if(node->pred_type == CHAR){
             //   cout << "Is there wrong? 5" << endl;
				predicate_string += node->preds[0].attr_name;
              //  cout << "Is there wrong? 6" << endl;
				predicate_string += rela_to_token(node->preds[0].rel);
              //  cout << "Is there wrong? 7" << endl;
				predicate_string += node->preds[0].value;
        //        cout << "Is there wrong? 8" << endl;
            }

		result_sql += predicate_string;
		result_sql += ';';

        //cout << "result_sql:" << result_sql << endl;
		// end of getting sql
		// start to transfer the sql to local site, and get result after it exuted.
		
		int excute_site = frag_information.site_id;
       // cout << to_string(frag_information.size-2) << endl;
		int result_frag_id = transfer_sql(result_sql, MAIN_SITE_ID, excute_site,table_names);

        // change the node_type from select to fragment.
        if(node->parent){
            query_tree_node* new_node = new query_tree_node();
            new_node->node_type = FRAGMENT;
            new_node->frag_id = result_frag_id;
            node->parent->child[child_id] = new_node;
            }
		else{
            vector<Operator> tmp = this->plan[excute_site];
            this->plan[excute_site][tmp.size()-1].is_end = 1;
            this->transfer_plan();
        }
        // cout << node->parent<< endl;
        // for(int i = 0;i < node->parent->child.size(); i++)
        //     cout << node->parent->child[i]->get_str() << endl;
	}

	// project: projectNode to sql,transfer,
	// select attribute
	// from table_name(find_table_name from frag_id)

	// child_num = 1
	// attribute_num = n
	if(node_type == PROJECT and node->child.size() != 1){
		// get sql
		string result_sql = "select ";
        cout << "Is wrong? 1" << endl;
        for(int attributeIndex = 0; attributeIndex < node->attr_names.size(); attributeIndex++){
        	result_sql += node->attr_names[attributeIndex];
            cout << "result_sql" + result_sql << endl;
        	if(attributeIndex < node->attr_names.size() -1){
        	    result_sql += ',';
            }
            else{
            	result_sql += ' ';
            }

        }
        cout << "Is wrong? 2" << endl;
        // select at1,at2 ;
        result_sql += 'from ';
        // select at1,at2 from ;
        query_tree_node* frag_node = node->child[0];
        cout << "Is wrong? 2w42" << endl;
        cout << to_string(frag_node->frag_id) << endl;
        frag_info frag_information = get_frag_info(frag_node->frag_id);
		cout << "Is wrong? qwewwqe" << endl;
        string table_name = get_frag_name(frag_information.frag_id);
        cout << "Is wrong? gfdsfdsf" << endl;
		result_sql += table_name;
		result_sql += ';';
		// end getting sql
		// start to transfer the sql to local site, and get result after it excuted;
		cout << "Is wrong? qrewter" << endl;
		int excute_site = frag_information.site_id;
		cout << "Is wrong? qwrefdfffghg" << endl;
        vector<string> table_names;
        table_names.push_back(table_name);
         cout << "Is wrong? e" << endl;
		int result_frag_id = transfer_sql(result_sql, MAIN_SITE_ID, excute_site,table_names);
		 cout << "Is wrong? r" << endl;
		// change the node type from project to fragment;
        if(node->parent){
        	 cout << "Is wrong? y" << endl;
            query_tree_node* new_node = new query_tree_node();
            new_node->node_type = FRAGMENT;
            new_node->frag_id = result_frag_id;
            vector<query_tree_node*> temp = node->parent->child;
            node->parent->child[child_id] = new_node;
        }
        else{
        	 cout << "Is wrong? q" << endl;
            vector<Operator> tmp = this->plan[excute_site];
            cout << ">><<<>><<" << endl;
            cout << to_string(plan[excute_site].size()) << endl;
            this->plan[excute_site][tmp.size()-1].is_end = 1;
            cout << "xfxfdfds" << endl;
            this->transfer_plan();
        }
    }
	if(node_type == PROJECT and node->child.size() == 1){
		cout << "xxddxxxxffffxvv" << endl;
		int frag_id = node->child[0]->frag_id;
		cout << "xxddxxxxffffxvvsdsadadasdasdas" << endl;
		for(int i=1 ; i<= 4; i++){
			cout << "xxddxxxxfasasasassafffxvv" << endl;
			if(plan[i].size() != 0){
				cout << to_string(plan[i].size()) << endl;
				if(plan[i][plan[i].size()-1].result_frag_id == frag_id){
					this->plan[i][plan[i].size()-1].is_end = 1;
					break;
					}
			}
		}
		this->transfer_plan();
		}
    // join: joinNode to sql(before excuted ,need to transfer table need join to one site, and need semi-join)
    // select *
    // from table_name1, table_name2
    // where table_name1.attribute = table_name2.attribute;
    if(node_type == JOIN){

    	query_tree_node* left_child = node->child[0];
    	query_tree_node* right_child = node->child[1];
    	//cout << "Is wrong? 1" << endl;
        // cout << node << endl;
        cout << "left_child:" + left_child->get_str() << endl;
        cout << "right_child:" + right_child->get_str() << endl;

    	frag_info left_fragment = get_frag_info(left_child->frag_id);
    	frag_info right_fragment = get_frag_info(right_child->frag_id);


        int left_site = left_fragment.site_id;
        int right_site = right_fragment.site_id;
        //cout << "Is wrong? 2" << endl;
        vector<int> frag_ids;
        //cout << to_string(left_child->frag_id) << "," << to_string(right_child->frag_id) << endl;
        frag_ids.push_back(left_child->frag_id);
        frag_ids.push_back(right_child->frag_id);

        int target_site_id;
        int new_frag_id;
        int other_frag_id;
        //cout << "Is wrong? 3" << endl;
        if (left_site != right_site){
            cout << "JOIN not on one site" << endl;
         //   cout << to_string(frag_ids[0]) << "," << to_string(frag_ids[1]) << endl;
        	// how to transfer: site_plan[0] is source, site_plan[1] is target.
        	vector<frag_info> frag_plan = this->how_to_transfer(frag_ids);
        	// transfer the table need join from source to target site.
        	// get parameters of transfer function.
       //     cout << "IS WRONG??" << endl;
            // cout <<  frag_plan[0].frag_id << endl;
            // cout << frag_plan[1].frag_id << endl;
            new_frag_id = frag_plan[0].frag_id;
            other_frag_id = frag_plan[1].frag_id;
            target_site_id = frag_plan[0].site_id;
      //      cout << "IS WRONG??  1" << endl;
        }
        else{
            target_site_id = left_site;
            new_frag_id = left_child->frag_id;
            other_frag_id = right_child->frag_id;
        }
        
        int result_frag_id;	
        int excute_site;
        if(this->s_join == true)
        	result_frag_id = this->semi_join(target_site_id, new_frag_id, other_frag_id);
        else{
        	//get sql(not semi join)
        //    cout << "IS WRONG??  2" << endl;
    		string result_sql = "select * from ";
            //cout << "new_frag_id:" << to_string(new_frag_id) << " ,other_frag_id:" + to_string(other_frag_id) << endl;
            frag_info frag_information_1 = get_frag_info(new_frag_id);
            frag_info frag_information_2 = get_frag_info(other_frag_id);
    		string table_name1 = get_frag_name(frag_information_1.frag_id);
	    	string table_name2 = get_frag_name(frag_information_2.frag_id);
	    	result_sql += table_name1;
	    	result_sql += ',';
	    	result_sql += table_name2;
	    	result_sql += " where ";
       //     cout << "IS WRONG??  3" << endl;
	    	// select * from table_name1,table_name2 where ;
	    	result_sql += table_name1;
	    	result_sql += '.';
        //    cout << "IS WRONG??  66" << endl;
          //  cout << node->get_str() << endl;
	    	result_sql += node->predt[0].left_attr;
	    	result_sql += rela_to_token(node->predt[0].rel);
	    	result_sql += table_name2;
	    	result_sql += '.';
        //    cout << "IS WRONG??  77" << endl;
	    	result_sql += node->predt[0].right_attr;
	    	result_sql += ';';

        //    cout << "IS WRONG??  4" << endl;
	    	//end getting sql;
	        //get transfer the table to one site;
	    	// start to transfer the sql to local site, and get result after it excuted;
       //     cout << "IS WRONG??  ?????" << endl;
      //      cout << "sql:" + result_sql + " ,excute_site:" + to_string(target_site_id) + ", size:" + to_string(frag_information_1.size-2);
			excute_site = target_site_id;

            vector<string> table_names;
            table_names.push_back(table_name1);
            table_names.push_back(table_name2);        //    cout << "sql:" + result_sql + " ,excute_site:" + to_string(excute_site) + ", size:" + to_string(frag_information_1.size-2);
			result_frag_id = transfer_sql(result_sql, MAIN_SITE_ID, excute_site,table_names);
         //   cout << "IS WRONG??  5" << endl;
        }
      //  cout << to_string(result_frag_id) << endl;
        if(node->parent and node->parent->child.size() != 1){
            query_tree_node* new_node = new query_tree_node();
            new_node->node_type = FRAGMENT;
            new_node->frag_id = result_frag_id;
            node->parent->child[child_id] = new_node;
            }
        else{
            vector<Operator> tmp = this->plan[excute_site];
            this->plan[excute_site][tmp.size()-1].is_end = 1;
            this->transfer_plan();
        }
        
        // cout << node->parent<< endl;
        // for(int i = 0;i < node->parent->child.size(); i++)
        //     cout << node->parent->child[i]->get_str() << endl;
    }
    
    // 
    if(node_type == UNION and node->parent->child.size() != 1){
    	bool Is_on_one_site = true;
    	int same_site = 0;
        //cout << "Is there wrong? 1" << endl;
    	for(int childIndex = 0; childIndex < node->child.size(); childIndex++){
    		query_tree_node* frag_node = node->child[childIndex];
    		frag_info one_child_fragment = get_frag_info(frag_node->frag_id);
    		if(childIndex == 0){
    			same_site = one_child_fragment.site_id;
    		}
    		else{
    			if(one_child_fragment.site_id != same_site){
    				Is_on_one_site = false;
    				break;
    			}

    		}
    	}
       // cout << "Is there wrong? 2" << endl;
        int target_site_id;
        string result_sql = "select * from ";

        int all_num = 0;
        vector<string> table_names;
    	if(Is_on_one_site == false){
    		vector<int> childs_frag_ids;
            //cout << "node_child_size:" + to_string(node->child.size()) << endl;
    		for(int childIndex = 0; childIndex < node->child.size(); childIndex++){
               // cout << node->child[childIndex]->frag_id << endl;
                childs_frag_ids.push_back(node->child[childIndex]->frag_id);
              //  cout << to_string(childs_frag_ids[childIndex]) << endl;
                all_num += get_frag_info(childs_frag_ids[childIndex]).size;
            }
       //     cout << "before how_to_transfer..." << endl;
    		vector<frag_info> frag_plan = this->how_to_transfer(childs_frag_ids);
        //    cout << "end how_to_transfer..." << endl;
            target_site_id = frag_plan[0].site_id;
            for(int childIndex = 0; childIndex < frag_plan.size(); childIndex++){
                frag_info one_child_fragment = frag_plan[childIndex];
                string table_name = get_frag_name(one_child_fragment.frag_id);
                result_sql += table_name;
                table_names.push_back(table_name);
                if(childIndex == node->child.size() -1)
                    result_sql += ";";
                else
                    result_sql += " union all select * from ";
            }
        //    cout << "target_site_id:" + to_string(target_site_id) << endl; 
    	}
    	else{
    		target_site_id = same_site;
            for(int childIndex = 0; childIndex < node->child.size(); childIndex++){
                query_tree_node* frag_node = node->child[childIndex];
                frag_info one_child_fragment = get_frag_info(frag_node->frag_id);
                all_num += one_child_fragment.size;
                string table_name = get_frag_name(one_child_fragment.frag_id);
                result_sql += table_name;
                table_names.push_back(table_name);
                if(childIndex == node->child.size() -1)
                    result_sql += ";";
                else
                    result_sql += " union all select * from ";
            }
    	}
       // cout << "Is there wrong? 3" << endl;
    	// get sql
    	
    	
     //   cout << "Is there wrong? 4" << endl;
    	//end getting sql;
	    //get transfer the table to one site;
	    // start to transfer the sql to local site, and get result after it excuted;
		int excute_site = target_site_id;
		int result_frag_id =  transfer_sql(result_sql, MAIN_SITE_ID, excute_site,table_names);

        if(node->parent){
            query_tree_node* new_node = new query_tree_node();
            new_node->node_type = FRAGMENT;
            new_node->frag_id = result_frag_id;
            node->parent->child[child_id] = new_node;
            }
        else{
            vector<Operator> tmp = this->plan[excute_site];
            this->plan[excute_site][tmp.size()-1].is_end = 1;
            this->transfer_plan();
        }
        // cout << node->parent<< endl;
        // for(int i = 0;i < node->parent->child.size(); i++)
        //     cout << node->parent->child[i]->get_str() << endl;
    }
    if(node_type == UNION and node->child.size() == 1){
    		cout << "dfsdfsdfdsfsd" << endl;
    		node->parent->child[0] = node->child[0];
    	}
}


// hou xu bian li qury tree
void query_plan::excute_query_plan(query_tree_node* node, int child_id)
{
//    cout << node->get_str() << endl;
    cout << "-----------starting recursion-----------" + node->get_str() << endl;

//    cout << "node.child.size():" + to_string(node->child.size()) << endl;
    //cout << "last_child:" + node->child[node->child.size()-1]->get_str() << endl;
	for(int child_index = 0; child_index < node->child.size(); child_index++){
  //      cout << node->get_str()+" child :" + node->child[child_index]->get_str() +"and child_index:" + to_string(child_index) << endl;
     //   cout << "child_index:"+ to_string(child_index) << endl;
		if(node->child[child_index]->node_type == SELECT or node->child[child_index]->node_type == PROJECT or node->child[child_index]->node_type == JOIN or node->child[child_index]->node_type == UNION){
			excute_query_plan(node->child[child_index], child_index);
      //      cout << to_string(node->child.size()) << endl;
       //     cout << "child_index:"+ to_string(child_index) << endl;
		  }
        }

//    cout << "end bianli child "+ node->get_str() << endl;
	// if this node is the leaf node(all childs are table)
	excute_one_operator(node, child_id);
	
}

void query_plan::start_excute(){
    cout << "-----------starting excute-----------" << endl;
    excute_query_plan(root, 0);
}

// int query_plan::load_datafile(string table_name, vector<string> attr_names, vector<string> attr_types, string key, string sql_file,int site){
//     transferred = this->mysql.load_datafile(table_name, attr_names, attr_types,key,sql_file);
//     return transferred;
// }



int query_plan::allocate(string fragment_name, int site, map<string,int> frag_to_id){
    int frag_id = frag_to_id[fragment_name];
    ///
    int size = 0;
    ///
    int transferred = this->transfer_table(fragment_name,0,site);
    return transferred;
}
// next week's work:
// fill the empty function;
// finish local excute code;
