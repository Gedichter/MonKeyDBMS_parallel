//
//  main.cpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/21/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include <iostream>
#include <string>
#include <fstream>
#include "LSM.hpp"
#include "Tree.hpp"
#include "Bloom_Filter.hpp"
#include <chrono>
//#include <thread>
#include "Multi_thread_unit_test.hpp"

using namespace std::chrono;

std::string workload0 = "workload_balance_0.txt";
std::string workload1 = "workload_balance_1.txt";
std::string workload2 = "workload_balance_2.txt";
std::string workload3 = "workload_balance_3.txt";

void workload(Tree* my_tree, std::string file_name){
    std::ifstream file(file_name);
    char action;
    int key, value;
    std::string out_name = "out_" + file_name;
    std::ofstream output(out_name);
    if (file.is_open()) {
        while (!file.eof()) {
            file >> action;
            if (action == 'p') {
                file >> key;
                file >> value;
                my_tree->put(key, value);
            }else if (action == 'g') {
                file >> key;
                int query = NULL;
                if(my_tree->get(key, query)){
                    output<<query<<std::endl;
                }else{
                    output<<std::endl;
                }
            }else if (action == 'd') {
                file >> key;
                my_tree->del(key);
            }else if (action == 'r'){
                int low, high;
                file >> low;
                file >> high;
                std::vector<KVpair> res = my_tree->range(low, high);
                for(int i = 0; i < res.size(); i++){
                    output<< res.at(i).key << ":"<<res.at(i).value<<" ";
                }
                output<<std::endl;
            }else{
                output<< "Error";
            }
        }
        file.close();
    }
    output.close();
}

void serial_evaluation(){
    Tree* my_tree = new Tree();
    std::cout<< "-----------serial evaluation------------------------" << std::endl;
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    workload(my_tree, workload0);
    workload(my_tree, workload1);
    workload(my_tree, workload2);
    workload(my_tree, workload3);
    high_resolution_clock::time_point t2 = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>( t2 - t1).count();
    std::cout<< "The elapsed time is " << duration << " microseconds" << std::endl;
}

void thread_evaluation(){
    Tree* my_tree = new Tree();
    std::cout<< "-----------multi-thread evaluation------------------------" << std::endl;
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    std::thread thread0(workload, my_tree, workload0);
    std::thread thread1(workload, my_tree, workload1);
    std::thread thread2(workload, my_tree, workload2);
    std::thread thread3(workload, my_tree, workload3);
    
    thread0.join();
    thread1.join();
    thread2.join();
    thread3.join();
    
    high_resolution_clock::time_point t2 = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>( t2 - t1).count();
    std::cout<< "The elapsed time is " << duration << " microseconds" << std::endl;
}


std::string no_delete_0 = "workload_no_delete_0.txt";
std::string no_delete_1 = "workload_no_delete_1.txt";
std::string all_read = "workload_all_read.txt";

void black_box_test(){
    Tree* my_tree = new Tree();
    std::thread thread1(workload, my_tree, no_delete_0);
    std::thread thread2(workload, my_tree, no_delete_1);
    thread1.join();
    thread2.join();
    
    workload(my_tree, all_read);
}

int main(int argc, const char * argv[]) {
    //main_test();
    //multi_thread_test(false);
    //concurrent_read();
    //concurrent_write(false);
    //read_then_write();
    //write_then_read(true);
    //serial_evaluation();
    //thread_evaluation();
    //black_box_test();
}





