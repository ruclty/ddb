#include "gdd.h"
#include <iostream>
using namespace std;

site_info GDD::get_site_info(int site_id){
    return SITES[site_id];
}

frag_info GDD::get_frag_info(int frag_id){
    return this->FRAGMENTS[frag_id];
}

table_info GDD::get_table_info(string table_name){
    return TABLES[table_name];
}

attr_info GDD::get_attr_info(string attr_name){
    return ATTRIBUTES[attr_name];
}

void GDD::generate_gdd(){
    for(int i = 1; i <= 4;i ++){
        site_info site = site_info{i};
        SITES[i] = site;
    }
    table_info* table;
    table = new table_info;
    table->table_name = "student";
    TABLES["student"] = *table;

    table = new table_info;
    table->table_name = "teacher";
    TABLES["teacher"] = *table;

    table = new table_info;
    table->table_name = "course";
    TABLES["course"] = *table;

    table = new table_info;
    table->table_name = "exam";
    TABLES["exam"] = *table;

    frag_info frag;
    predicateV predv;
    predicateV predv1;
    predicateV predv2;
    //fragments of student table
    frag = frag_info{1,H,"student",1};
    predv = predicateV{"student","id",L,1050000};
    frag.predv.push_back(predv);
    frag.size = 4;
    FRAGMENTS[1] = frag;
    TABLES["student"].h_frag.push_back(frag);
    SITES[1].fragment_ids.push_back(1);
    

    frag = frag_info{2,H,"student",2};
    predv1 = predicateV{"student","id",GE,1050000};
    predv2 = predicateV{"student","id",L,1100000};
    frag.size = 9;
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[2] = frag;
    TABLES["student"].h_frag.push_back(frag);
    SITES[2].fragment_ids.push_back(2);
    

    frag = frag_info{3,H,"student",3};
    predv = predicateV{"student","id",GE,1100000};
    frag.size = 6;
    frag.predv.push_back(predv);
    FRAGMENTS[3] = frag;
    TABLES["student"].h_frag.push_back(frag);
    SITES[3].fragment_ids.push_back(3);
    

    //fragments of teacher table
    frag = frag_info{4,H,"teacher",1};
    predv1 = predicateV{"teacher","id",L,2010000};
    predv2 = predicateV{"teacher","title",NE,3};
    frag.size = 54;
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[4] = frag;
    TABLES["teacher"].h_frag.push_back(frag);
    SITES[1].fragment_ids.push_back(4);

    frag = frag_info{5,H,"teacher",2};
    frag.size = 3;
    predv1 = predicateV{"teacher","id",L,2010000};
    predv2 = predicateV{"teacher","title",EQ,3};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[5] = frag;
    TABLES["teacher"].h_frag.push_back(frag);
    SITES[2].fragment_ids.push_back(5);
    

    frag = frag_info{6,H,"teacher",3};
    frag.size = 19;
    predv1 = predicateV{"teacher","id",GE,2010000};
    predv2 = predicateV{"teacher","title",NE,3};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[6] = frag;
    TABLES["teacher"].h_frag.push_back(frag);
    SITES[3].fragment_ids.push_back(6);
    

    frag = frag_info{7,H,"teacher",4};
    predv1 = predicateV{"teacher","id",GE,2010000};
    predv2 = predicateV{"teacher","title",EQ,3};
    frag.size = 10;
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[7] = frag;
    TABLES["teacher"].h_frag.push_back(frag);
    SITES[4].fragment_ids.push_back(7);
    


    //fragments of table course
    frag = frag_info{8,V,"course",1};
    frag.attr_names.push_back("id");
    frag.size = 8;
    frag.attr_names.push_back("name");
    FRAGMENTS[8] = frag;
    TABLES["course"].v_frag.push_back(frag);
    SITES[1].fragment_ids.push_back(8);
    

    frag = frag_info{9,V,"course",2};
    frag.size = 21;
    frag.attr_names.push_back("id");
    frag.attr_names.push_back("location");
    frag.attr_names.push_back("credit_hour");
    frag.attr_names.push_back("teacher_id");
    FRAGMENTS[9] = frag;
    TABLES["course"].v_frag.push_back(frag);
    SITES[2].fragment_ids.push_back(9);
    

    //fragments of exam table
    frag = frag_info{10,H,"exam",1};
    frag.size = 24;
    predv1 = predicateV{"exam","student_id",L,1070000};
    predv2 = predicateV{"exam","course_id",L,301200};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[10] = frag;
    TABLES["exam"].h_frag.push_back(frag);
    SITES[1].fragment_ids.push_back(10);

    frag = frag_info{11,H,"exam",2};
    frag.size = 14;
    predv1 = predicateV{"exam","student_id",L,1070000};
    predv2 = predicateV{"exam","course_id",GE,301200};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[11] = frag;
    TABLES["exam"].h_frag.push_back(frag);
    SITES[2].fragment_ids.push_back(11);
    

    frag = frag_info{12,H,"exam",3};
    frag.size = 17;
    predv1 = predicateV{"exam","student_id",GE,1070000};
    predv2 = predicateV{"exam","course_id",L,301200};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[12] = frag;
    TABLES["exam"].h_frag.push_back(frag);
    SITES[3].fragment_ids.push_back(12);
    

    frag = frag_info{13,H,"exam",4};
    frag.size = 12;
    predv1 = predicateV{"exam","student_id",GE,1070000};
    predv2 = predicateV{"exam","course_id",GE,301200};
    frag.predv.push_back(predv1);
    frag.predv.push_back(predv2);
    FRAGMENTS[13] = frag;
    TABLES["exam"].h_frag.push_back(frag);
    SITES[4].fragment_ids.push_back(13);
    
}

void GDD::print(){
    cout << "Fragments: " << endl;
    for (auto f: FRAGMENTS){
        cout << f.first << ":" << f.second.table_name << ", " << f.second.site_id << ":";
        if(f.second.frag_type == H){
            for(auto p: f.second.predv)
                cout << p.get_str() << "|";
        }
        else{
            for(auto a: f.second.attr_names)
                cout << a << "|";
        }
        cout << endl;
    }
    cout << "Sites:" << endl;
    for (auto s: SITES){
        cout << s.first  << ":" ;
        for (auto f:s.second.fragment_ids)
            cout << f << ", ";
        cout << endl;
    }
    cout << "Tables:" << endl;
    for (auto t: TABLES){
        cout << t.first  << ", ";
        cout << "h frags: ";
        for (auto f:t.second.h_frag)
            cout << f.frag_id << ", ";
        cout << "v frags: ";
        for (auto f:t.second.v_frag)
            cout << f.frag_id << ", ";
        cout << endl;
    }
}

int GDD::get_new_frag_id(){
    int new_frag_id = FRAGMENTS.size()+1;
    return new_frag_id;
}
void GDD::create_frag_info(frag_info new_frag_info){
    this->FRAGMENTS.insert(pair<int,frag_info>(new_frag_info.frag_id,new_frag_info));
}

int GDD::from_name_find_frag_id(string frag_name){
    for(int i=0;i<this->FRAGMENTS.size();i++){
        if(FRAGMENTS[i].table_name == frag_name)
            return i;
    }
}
void GDD::update_frag_info(int frag_id,vector<string> attr_names,vector<attr_info> attributes,int size){
    this->FRAGMENTS[frag_id].attr_names = attr_names;
    this->FRAGMENTS[frag_id].attributes = attributes;
    this->FRAGMENTS[frag_id].size = size;
}

void GDD::print_frag_info(frag_info frag_information){
    cout << "***frag_information of:" + to_string(frag_information.frag_id) + " frag_site:" + to_string(frag_information.site_id) << endl;

}