#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include <regex>
#include <queue>
#include <map>
#include "querytree.h"
#include "../gdd/gdd.h"
using namespace std;

bool conflict_node(query_tree_node* node, query_tree_node* root){
	for (auto predv1: node->predv)
		for(auto predv2: root->predv)
			if(predv1.table_name == predv2.table_name && predv1.attr_name == predv2.attr_name && conflict_pred(predv1, predv2))
				return true;
	for (auto preds1: node->preds)
		for (auto preds2: root->preds)
			if(conflict_pred(preds1, preds2))
				return true;
    for(auto child: root->child)
        if(conflict_node(node, child))
            return true;
	return false;
}

bool conflict_node(query_tree_node* node, query_tree_node* root, string l_t, string l_a, string r_t, string r_a){
	//std::cout << "\t"+node->get_str()+","+root->get_str()<< endl;
    for (auto predv1: node->predv)
		for(auto predv2: root->predv)
			if((predv1.table_name==l_t&&predv1.attr_name==l_a&&predv2.table_name==r_t&&predv2.attr_name==r_a)
                ||(predv1.table_name==r_t&&predv1.attr_name==r_a&&predv2.table_name==l_t&&predv2.attr_name==l_a))
				if(conflict_pred(predv1, predv2)){
                  //  std::cout << "\t"+node1->get_str()+","+node2->get_str()+", conflict!" << endl;
                    //cout << "\tconflict" << endl;
                    return true;
                }
	for (auto preds1: node->preds)
		for (auto preds2: root->preds)
			if(conflict_pred(preds1, preds2))
				return true;
    for (auto child: root->child)
        if(conflict_node(node, child, l_t, l_a, r_t, r_a)){
            //cout << "\tconflict" << endl;
            return true;
        }
   // std::cout << "\t"+node1->get_str()+","+node2->get_str()+", no conflict!" << endl;
    //cout << "\tno conflict" << endl;
	return false;
}

void query_tree_node::remove_null_child(){
    for(vector<query_tree_node*>::iterator i = child.begin(); i != child.end();)
        if(*i == NULL)
            child.erase(i);
        else
            i ++;
}

int query_tree_node::get_child_index(query_tree_node* child_node){
    for(int i = 0;i < child.size();i ++)
        if(child[i] == child_node)
            return i;
    return -1;
}

query_tree_node* query_tree_node::copy(){
    query_tree_node* copy_node = new query_tree_node;
    copy_node->table_names = table_names;
    copy_node->attr_names = attr_names;
    copy_node->node_type = node_type;
    copy_node->child = child;
    copy_node->pred_type = pred_type;
    copy_node->preds = preds;
    copy_node->predt = predt;
    copy_node->predv = predv;
    copy_node->parent = parent;
    copy_node->frag_id = frag_id;
    copy_node->frag_ids = frag_ids;
    for (int i = 0;i < copy_node->child.size();i ++){
        copy_node->child[i] = child[i]->copy();
        copy_node->child[i]->parent = copy_node;
    }
    return copy_node;
}

string query_tree_node::get_str(){
    string ntype = ntype_to_token(node_type);
    if(node_type == PROJECT){
        string proj_cond = "(|";
        for (int i = 0; i < attr_names.size(); i ++){
            proj_cond += table_names[i]+"."+attr_names[i]+"|";
        }
        proj_cond += ")";
        return ntype+proj_cond;
    }
    if(node_type == JOIN || node_type == SELECT){
        string predicate = "(|";
        switch(pred_type){
            case INT: for(auto pred : predv)
                        predicate += pred.get_str()+"|";
            case CHAR: for(auto pred : preds)
                        predicate += pred.get_str()+"|";
            case TAB: for(auto pred : predt)
                        predicate += pred.get_str()+"|"; 
        }
        predicate += ")";
        return ntype+predicate;
    }
    if(node_type == TABLE)
        return ntype+"("+table_names[0]+")";
    if(node_type == UNION){
        string tables = "(|";
        for(auto table_name:table_names){
            tables += table_name + "|";
        }
        return ntype+tables+")";
    }
    if(node_type == FRAGMENT){
        return ntype+"("+to_string(frag_id)+")";
    }
    return ntype;
}

query_tree::query_tree(string& sql) {
	this->sql = sql;
	this->root = NULL;
}

query_tree::~query_tree(){}

void query_tree::split_sql() {
    int sel_index = this->sql.find("select");
    int from_index = this->sql.find("from");
    int where_index = this->sql.find("where");

    this->sel_items = this->sql.substr(sel_index+6, from_index-sel_index-6);
    this->from_items = this->sql.substr(from_index+4, where_index-from_index-4);
    if (where_index > 0)
        this->where_items = this->sql.substr(where_index+5);
}
vector<string> query_tree::parser_regex(string str, string pattern){
    regex e(pattern);
    smatch sm;
    string::const_iterator start = str.begin();
    string::const_iterator end = str.end();
    vector<string> results;
    while(regex_search(start, end, sm, e)){
        string msg(sm[0].first, sm[0].second);
        results.push_back(msg);
        start = sm[0].second;
    }
    return results;
}

void query_tree::parser_sql() {
    vector<string> results;
    //cout << "--------split_sql---------" << endl;
    split_sql();

    //cout << this->sel_items <<"|"<< endl;
    //cout << this->from_items <<"|"<< endl;
    //cout << this->where_items <<"|"<< endl;

    //cout << "---------parser_sel---------" << endl;
    results = parser_regex(sel_items, "[\\w]+\\.[\\w]+");
    for (auto res:results)
        projects.push_back(res);
    //cout << "---------parser_from--------" << endl;
    trim(from_items);
    table_names = split(from_items, ",");
    //cout << "---------parser_where--------" << endl;
    if(where_items.size() != 0){
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\=[0-9]+");
        for (auto res:results)
            extract_pred(res, "=", EQ, INT);
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\>[0-9]+");
        for (auto res:results)
            extract_pred(res, ">",G, INT);
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\<[0-9]+");
        for (auto res:results)
            extract_pred(res, "<",L, INT);
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\>=[0-9]+");
        for (auto res:results)
            extract_pred(res, ">=", GE, INT);
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\<=[0-9]+");
        for (auto res:results)
            extract_pred(res, "<=", LE, INT);
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\!=[0-9]+");
        for (auto res:results)
            extract_pred(res, "!=", NE, INT);
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\!=\\'[^#]*\\'");
        for (auto res:results)
            extract_pred(res, "!=", NE, CHAR);
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\=\\'[^#]*\\'");
        for (auto res:results)
            extract_pred(res, "=",EQ, CHAR);
        results = parser_regex(where_items, "[\\w]+\\.[\\w]+\\=[\\w]+\\.[\\w]+");
        for (auto res:results)
            extract_pred(res, "=",EQ, TAB);
    }
    /*for (vector<predicateV>::iterator iter = predv.begin(); iter != predv.end(); iter ++){
        cout << iter->get_str() << endl;
    }
    for (vector<predicateS>::iterator iter = preds.begin(); iter != preds.end(); iter ++){
        cout << iter->get_str() << endl;
    }
    for (vector<predicateT>::iterator iter = predt.begin(); iter != predt.end(); iter ++){
        cout << iter->get_str() << endl;
    }*/
}

void query_tree::extract_pred(string str, string tok, RELATION rel, PREDTYPE pred_type){
    int index = str.find_first_of(tok);
    string left = str.substr(0, index);
    string right = str.substr(index+tok.size());
    index = left.find_first_of(".");
    string table = left.substr(0,index);
    string attribute = left.substr(index+1);
    struct predicateV pred1;
    struct predicateS pred2;
    struct predicateT pred3;
    switch(pred_type){
        case INT:
            pred1.rel = rel;
            pred1.value = atoi(right.c_str());
            pred1.table_name = table;
            pred1.attr_name = attribute;
            predv.push_back(pred1);
            break;
        case CHAR:
            pred2.rel = rel;
            pred2.table_name = table;
            pred2.attr_name = attribute;
            pred2.value = right;
            preds.push_back(pred2);
            break;
        case TAB:
            pred3.rel = rel;
            pred3.left_table = table;
            pred3.left_attr = attribute;
            index = right.find_first_of(".");
            pred3.right_table = right.substr(0, index);
            pred3.right_attr = right.substr(index+1);
            predt.push_back(pred3);
    }
}

void query_tree::generate_tree() {
   // cout << "build join node" << endl;
    map<string, query_tree_node*> table2node;
    for(auto table : table_names){
        query_tree_node* node = new query_tree_node;
        node->node_type = TABLE;
        node->table_names.push_back(table);
        node->parent = NULL;
        table2node[table] = node;
        leaf_table[table] = node;
    }
    query_tree_node* join_root = NULL;
    for(auto pred: predt){
        string left_table = pred.left_table;
        string right_table = pred.right_table;
        query_tree_node* left_node = table2node[left_table];
        query_tree_node* right_node = table2node[right_table];
        if(left_node != right_node){
            query_tree_node* node = new query_tree_node;
            node->node_type = JOIN;
            node->pred_type = TAB;
            node->predt.push_back(pred);
            node->child.push_back(table2node[left_table]);
            node->child.push_back(table2node[right_table]);
            node->table_names.push_back(left_table);
            node->table_names.push_back(right_table);
            left_node->parent = node;
            right_node->parent = node;
            table2node[left_table] = node;
            table2node[right_table] = node;
            if(join_root == left_node || join_root == right_node || join_root == NULL)
                join_root = node; 
            join_nodes.push_back(node);
        }
        root = join_root;
    }
    if(root == NULL){
        root = leaf_table[table_names[0]];
    }
    //cout << " build select node" << endl;
    for(auto pred: preds){
        query_tree_node* node = new query_tree_node;
        node->node_type = SELECT;
        node->pred_type = CHAR;
        node->preds.push_back(pred);
        node->parent = NULL;
        node->child.push_back(root);
        root->parent = node;
        root = node;
        node->table_names.push_back(pred.table_name);
        node->attr_names.push_back(pred.attr_name);
        sel_nodes.push_back(node);
    }
    for(auto pred: predv){
        query_tree_node* node = new query_tree_node;
        node->node_type = SELECT;
        node->pred_type = INT;
        node->predv.push_back(pred);
        node->parent = NULL;
        node->child.push_back(root);
        root->parent = node;
        root = node;
        node->table_names.push_back(pred.table_name);
        node->attr_names.push_back(pred.attr_name);
        sel_nodes.push_back(node);
    }

  //  cout << "build project node" << endl;
    query_tree_node* node = new query_tree_node;
    node->node_type = PROJECT;
    if(projects.empty()){
        node->table_names.push_back("");
        node->attr_names.push_back("*");
    }
    else
        for(auto p : projects){
            int index = p.find_first_of(".");
            node->table_names.push_back(p.substr(0,index));
            node->attr_names.push_back(p.substr(index+1));
        }
    node->parent = NULL;
    node->child.push_back(root);
    root->parent = node;
    root = node;
}

void optimize(query_tree_node* node){}
void optimize(query_tree_node* node1, query_tree_node* node2){}

// print tree
void query_tree::print_tree(){
    cout << "-----------print tree---------" << endl << endl;
    queue<query_tree_node*> q;
    queue<int> deep;
    query_tree_node* last = NULL;
    int last_d;
    q.push(root);
    deep.push(0);
    while(!q.empty()){
        query_tree_node* node = q.front();
        int d = deep.front();
        for(auto n : node->child){
            q.push(n);
            deep.push(d+1);
        }
        if(last != NULL && node->parent != last->parent)
            cout << "||";
        else if(last != NULL)
            cout << "\t";
        if(last != NULL && d != last_d)
            cout << endl << endl;
        cout << node->get_str();
        //if(node->parent)
         //   cout << ","<<node->parent;
        q.pop();
        deep.pop();
        last = node;
        last_d = d;
    }
    cout << endl << endl;
    cout << "------------------------------" << endl;
}
//
void query_tree::frag_table(){
    for(auto join:join_nodes){
        for (int i = 0;i < join->child.size();i ++){
            query_tree_node* child = join->child[i];
            if(child->node_type == TABLE){
                table_info table = get_table_info(child->table_names[0]);
                if(table.h_frags.size() > 0){
                    query_tree_node* union_node = new query_tree_node;
                    union_node->node_type = UNION;
                    union_node->table_names.push_back(table.table_name);
                    union_node->parent = join;
                    for(auto frag_id:table.h_frags){
                        frag_info frag = get_frag_info(frag_id);
                        query_tree_node* frag_node = new query_tree_node;
                        frag_node->node_type = FRAGMENT;
                        frag_node->frag_id = frag.frag_id;
                        frag_node->parent = union_node;
                        frag_node->preds = frag.preds;
                        frag_node->predv = frag.predv;
                        union_node->child.push_back(frag_node);
                        frag_nodes.push_back(frag_node);
                    }
                    join->child[i] = union_node;
                }
                if(table.v_frags.size() > 0){
                    query_tree_node* join_node = new query_tree_node;
                    join_node->node_type = JOIN;
                    join_node->table_names.push_back(table.table_name);
                    join_node->parent = join;
                    query_tree_node* l_frag = new query_tree_node;
                    frag_info v_frag0 = get_frag_info(table.v_frags[0]);
                    frag_info v_frag1 = get_frag_info(table.v_frags[1]);
                    l_frag->frag_id = v_frag0.frag_id;
                    l_frag->node_type = FRAGMENT;
                    l_frag->parent = join_node;
                    join_node->child.push_back(l_frag);
                    frag_nodes.push_back(l_frag);
                    query_tree_node* r_frag = new query_tree_node;
                    r_frag->frag_id = v_frag1.frag_id;
                    r_frag->node_type = FRAGMENT;
                    r_frag->parent = join_node;
                    frag_nodes.push_back(r_frag);
                    join_node->child.push_back(r_frag);

                    string key = get_table_info(v_frag0.table_name).key;
                    predicateT pred = {v_frag0.table_name, key, 
                                        EQ, key, v_frag1.table_name};
                    join_node->pred_type = TAB;
                    join_node->predt.push_back(pred);
                    join->child[i] = join_node;
                }
            }
        }
    }
}

void query_tree::push_select(){
    for (auto sel: sel_nodes){
        sel->parent->child[0] = sel->child[0];
        sel->child[0]->parent = sel->parent;
        sel->parent = NULL;
        sel->child.clear();
        for(auto frag: frag_nodes){
            frag_info frag_ = get_frag_info(frag->frag_id);
            int index = frag->parent->get_child_index(frag);
            if(sel->table_names[0] != frag_.table_name)
                continue;
            if(frag_.frag_type == H){
                if(conflict_node(sel, frag) || conflict_node(frag, sel)){
                    frag->parent->child[index] = NULL;
                    frag->parent->remove_null_child();
                }
                else{
                    query_tree_node* new_sel = sel->copy();
                    new_sel->child.clear();
                    new_sel->parent = frag->parent;
                    new_sel->parent->child[index] = new_sel;
                    frag->parent = new_sel;
                    new_sel->child.push_back(frag);
                }
            }
            else{
                for(auto attr:frag_.attr_names){
                    if(attr == sel->attr_names[0]){
                        query_tree_node* new_sel = sel->copy();
                        new_sel->child.clear();
                        new_sel->parent = frag->parent;
                        new_sel->parent->child[index] = new_sel;
                        frag->parent = new_sel;
                        new_sel->child.push_back(frag);
                        break;
                    }
                }
            }
        }
    }
}

void query_tree::prune_join_node(query_tree_node* node){
    if(node->node_type != JOIN)
        for(auto child_node: node->child)
            prune_join_node(child_node);
    else{
        if (node->child[0]->node_type == JOIN)
            prune_join_node(node->child[0]);
        if (node->child[1]->node_type == JOIN)
            prune_join_node(node->child[1]);
        vector<query_tree_node*> temp_join;
        if (node->child[0]->node_type == UNION && node->child[1]->node_type != UNION){
            for(auto child_node: node->child[0]->child){
                bool flag = true;
                for(auto pred: node->predt){
                    if(conflict_node(child_node, node->child[1], pred.left_table, pred.left_attr, pred.right_table, pred.right_attr)
                     || conflict_node(node->child[1], child_node, pred.left_table, pred.left_attr, pred.right_table, pred.right_attr)){
                        flag = false;
                        break;
                    }
                }
                if (!flag)
                    continue;
                query_tree_node* new_child = node->child[1]->copy();
                query_tree_node* new_join = node->copy();
                new_join->child.clear();
                new_join->child.push_back(child_node);
                new_join->child.push_back(new_child);
                child_node->parent = new_join;
                new_child->parent = new_join;
                temp_join.push_back(new_join);
            }
        }
        else if(node->child[0]->node_type == UNION && node->child[1]->node_type == UNION){
            for(auto child_node1 : node->child[0]->child){
                for(auto child_node2 : node->child[1]->child){
                    bool flag = true;
                    for(auto pred: node->predt){
                        if(conflict_node(child_node1, child_node2, pred.left_table, pred.left_attr, pred.right_table, pred.right_attr)
                        ||conflict_node(child_node2, child_node1, pred.left_table, pred.left_attr, pred.right_table, pred.right_attr)){
                            flag = false;
                            break;
                        }
                    }
                    if (!flag)
                        continue;
                    query_tree_node* new_child1 = child_node1->copy();
                    query_tree_node* new_child2 = child_node2->copy();
                    query_tree_node* new_join = node->copy();
                    new_join->child.clear();
                    new_join->child.push_back(new_child1);
                    new_join->child.push_back(new_child2);
                    new_child1->parent = new_join;
                    new_child2->parent = new_join;
                    temp_join.push_back(new_join);
                }
            }
        }
        else if(node->child[0]->node_type != UNION && node->child[1]->node_type == UNION){
            for(auto child_node: node->child[1]->child){
                bool flag = true;
                for(auto pred: node->predt){
                    if(conflict_node(child_node, node->child[0], pred.left_table, pred.left_attr, pred.right_table, pred.right_attr)
                    ||conflict_node(node->child[0], child_node, pred.left_table, pred.left_attr, pred.right_table, pred.right_attr)){
                        flag = false;
                        break;
                    }
                }
                if (!flag)
                    continue;
                query_tree_node* new_child = node->child[0]->copy();
                query_tree_node* new_join = node->copy();
                new_join->child.clear();
                new_join->child.push_back(child_node);
                new_join->child.push_back(new_child);
                child_node->parent = new_join;
                new_child->parent = new_join;
                temp_join.push_back(new_join);
            }
        }
        if(!temp_join.empty()){
            query_tree_node* union_node = new query_tree_node;
            union_node->node_type = UNION;
            for(auto join_node: temp_join){
                join_node->parent = union_node;
                union_node->child.push_back(join_node);
            }
            union_node->parent = node->parent;
            int index = node->parent->get_child_index(node);
            node->parent->child[index] = union_node;
        }
    }
}
void query_tree::prune_join(){
    prune_join_node(root);
}

string query_tree::get_sql(){
    return this->sql;
}
query_tree_node* query_tree::get_root(){
    return this->root;
}
