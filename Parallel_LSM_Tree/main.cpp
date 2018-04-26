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

void bloomfilter_test(){
    BloomFilter bl = BloomFilter(20000, 0.01);
    for(int i = 0; i < 5000; i+=2){
        bl.add(i);
    }
    bool tp = true;
    for(int i = 0; i < 5000; i+= 2){
        if(!bl.possiblyContains(i)){
            tp = false;
            std::cout<<"false negative!"<<std::endl;
        }
    }
    if(tp){
        std::cout<<"No false negative!"<<std::endl;
    }
    bool tn = true;
    for(int i = 10000; i < 20000; i+=1){
        if(bl.possiblyContains(i)){
            tn = false;
            std::cout<<"false positive!"<<std::endl;
        }
    }
    if(tn){
        std::cout<<"No false positive!"<<std::endl;
    }
    
    bl.reset();
    bool rs = true;
    for(int i = 0; i < 5000; i+= 2){
        if(bl.possiblyContains(i)){
            rs = false;
            std::cout<<"Reset not succeesful!"<<std::endl;
        }
    }
    if(rs){
        std::cout<<"Reset succeesful!"<<std::endl;
    }
}

void read_file(std::string name, unsigned long size){
    std::ifstream file(name, std::ios::binary);
    KVpair* array = new KVpair[size];
    file.read((char*)array, size*sizeof(KVpair));
    for(int i = 0; i < size; i++){
        std::cout<< "one new KV entry"<<std::endl;
        std::cout<< array[i].key <<std::endl;
        std::cout<< array[i].value <<std::endl;
        std::cout<< array[i].del <<std::endl;
    }
}


void create_file(){
    std::string name = "yourFile";
    std::ofstream stream(name.c_str(), std::ios::binary);
    KVpair array[2];
    array[0] = {3,4,true};
    array[1] = {7,8,false};
    stream.write((char*) array, 2*sizeof(KVpair));
    stream.close();
    std::cout<<"finished"<<std::endl;
    read_file(name, 2);
}

void range_test(){
    Tree my_tree;
    for(int i = 0; i < 400; i+=2){
        my_tree.put(i, i-1);
    }
    std::vector<KVpair> res = my_tree.range(100, 150);
    for(int i = 0; i < res.size(); i++){
        std::cout << res.at(i).key << ":"<<res.at(i).value<<"  ";
    }
    std::cout<<std::endl;
    
}

void tree_test(){
    Tree my_tree;
    for(int i = 0; i < 4000; i++){
        my_tree.put(i, i-1);
    }
    for(int i = 0; i < 400; i+=2){
        my_tree.put(i, i);
    }
    for(int i = 0; i < 100; i+=1){
        my_tree.del(i);
    }
    for(int i = 0; i < 50; i++){
        my_tree.put(i, i+5);
    }
    int query = NULL;
    for(int i = 40; i < 60; i++){
        if(my_tree.get(i, query)){
            std::cout << "The new value is " <<query<<std::endl;
        }else{
            std::cout << "Point lookup not found "<<std::endl;
        }
    }
    for(int i = 100; i < 120; i++){
        if(my_tree.get(i, query)){
            std::cout << "The new value is " <<query<<std::endl;
        }else{
            std::cout << "Point lookup not found "<<std::endl;
        }
    }
//    for(int i = 0; i < my_tree.layers.size();i++){
//        std::cout<<"the layer "<<i<<std::endl;
//        for(int j = 0; j < parameters::NUM_RUNS; j++){
//            if(my_tree.layers.at(i).run_size[j] != 0){
//                std::cout<<"In the file "<<my_tree.layers.at(i).get_name(j)<<std::endl;
//                read_file(my_tree.layers.at(i).get_name(j), my_tree.layers.at(i).run_size[j]);
//            }
//        }
//    }
}

void tree_test_2(){
    Tree my_tree;
    for(int i = 0; i < 10; i++){
        my_tree.put(i, i-1);
    }
    for(int i = 5; i < 15; i+=1){
        my_tree.del(i);
    }
    int query = NULL;
    for(int i = 0; i < 15; i++){
        if(my_tree.get(i, query)){
            std::cout << "The new value is " <<query<<std::endl;
        }else{
            std::cout << "Point lookup not found "<<std::endl;
        }
    }
    //    for(int i = 0; i < my_tree.layers.size();i++){
    //        std::cout<<"the layer "<<i<<std::endl;
    //        for(int j = 0; j < parameters::NUM_RUNS; j++){
    //            if(my_tree.layers.at(i).run_size[j] != 0){
    //                std::cout<<"In the file "<<my_tree.layers.at(i).get_name(j)<<std::endl;
    //                read_file(my_tree.layers.at(i).get_name(j), my_tree.layers.at(i).run_size[j]);
    //            }
    //        }
    //    }
}


/** Multi-threading querying**/
std::string workload1 = "workload_balance_1.txt";
std::string workload2 = "workload_balance_2.txt";

void main_test(){
    Tree my_tree;
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    std::ifstream file (workload1);
    char action;
    int key, value;
    
    std::ofstream output_file("out_single.txt");

    if (file.is_open()) {
        while (!file.eof()) {
            file >> action;
            if (action == 'p') {
                file >> key;
                file >> value;
                my_tree.put(key, value);
            }
            else if (action == 'g') {
                file >> key;
                int query = NULL;
                if(my_tree.get(key, query)){
                    output_file<<query<<std::endl;
                }else{
                    output_file<<std::endl;
                }
            }else if (action == 'd') {
                file >> key;
                my_tree.del(key);
            }else if (action == 'r'){
                int low, high;
                file >> low;
                file >> high;
                std::vector<KVpair> res = my_tree.range(low, high);
                for(int i = 0; i < res.size(); i++){
                    output_file<< res.at(i).key << ":"<<res.at(i).value<<" ";
                }
                output_file<<std::endl;
            }else{
                output_file<< "Error";
            }
        }
        file.close();
    }
    file = std::ifstream(workload2);
    if (file.is_open()) {
        while (!file.eof()) {
            file >> action;
            if (action == 'p') {
                file >> key;
                file >> value;
                my_tree.put(key, value);
            }
            else if (action == 'g') {
                file >> key;
                int query = NULL;
                if(my_tree.get(key, query)){
                    output_file<<query<<std::endl;
                }else{
                    output_file<<std::endl;
                }
            }else if (action == 'd') {
                file >> key;
                my_tree.del(key);
            }else if (action == 'r'){
                int low, high;
                file >> low;
                file >> high;
                std::vector<KVpair> res = my_tree.range(low, high);
                for(int i = 0; i < res.size(); i++){
                    output_file<< res.at(i).key << ":"<<res.at(i).value<<" ";
                }
                output_file<<std::endl;
            }else{
                output_file<< "Error";
            }
        }
        file.close();
    }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>( t2 - t1).count();
        std::cout<< "-----------------------------------------------------" << std::endl;
        std::cout<< "The elapsed time is " << duration << " microseconds" << std::endl;
        
}

std::shared_timed_mutex global_mutex;

void execute_workload(Tree* my_tree){
    std::ifstream file(workload1);
    char action;
    int key, value;
    std::ofstream output("out_range_multi.txt");
    if (file.is_open()) {
        while (!file.eof()) {
            file >> action;
            if (action == 'p') {
                global_mutex.lock();
                file >> key;
                file >> value;
                my_tree->put(key, value);
                global_mutex.unlock();
            }else if (action == 'g') {
                global_mutex.lock_shared();
                file >> key;
                int query = NULL;
                if(my_tree->get(key, query)){
                    output<<query<<std::endl;
                }else{
                    output<<std::endl;
                }
                global_mutex.unlock_shared();
            }else if (action == 'd') {
                global_mutex.lock();
                file >> key;
                my_tree->del(key);
                global_mutex.unlock();
            }else if (action == 'r'){
                global_mutex.lock_shared();
                int low, high;
                file >> low;
                file >> high;
                std::vector<KVpair> res = my_tree->range(low, high);
                for(int i = 0; i < res.size(); i++){
                    output<< res.at(i).key << ":"<<res.at(i).value<<" ";
                }
                output<<std::endl;
                global_mutex.unlock_shared();
            }else{
                output<< "Error";
            }
        }
        file.close();
    }
    output.close();
}

void execute_workload1(Tree* my_tree){
    std::ifstream file(workload2);
    char action;
    int key, value;
    std::ofstream output("out_range_multi1.txt");
    if (file.is_open()) {
        while (!file.eof()) {
            file >> action;
            if (action == 'p') {
                global_mutex.lock();
                file >> key;
                file >> value;
                my_tree->put(key, value);
                global_mutex.unlock();
            }else if (action == 'g') {
                global_mutex.lock_shared();
                file >> key;
                int query = NULL;
                if(my_tree->get(key, query)){
                    output<<query<<std::endl;
                }else{
                    output<<std::endl;
                }
                global_mutex.unlock_shared();
            }else if (action == 'd') {
                global_mutex.lock();
                file >> key;
                my_tree->del(key);
                global_mutex.unlock();
            }else if (action == 'r'){
                global_mutex.lock_shared();
                int low, high;
                file >> low;
                file >> high;
                std::vector<KVpair> res = my_tree->range(low, high);
                for(int i = 0; i < res.size(); i++){
                    output<< res.at(i).key << ":"<<res.at(i).value<<" ";
                }
                output<<std::endl;
                global_mutex.unlock_shared();
            }else{
                output<< "Error";
            }
        }
        file.close();
    }
    output.close();
}

std::mutex cout_mutex;

void execute_workload_single_output(Tree* my_tree, std::ofstream* output){
    std::ifstream file(workload1);
    char action;
    int key, value;
    if (file.is_open()) {
        while (!file.eof()) {
            file >> action;
            if (action == 'p') {
                global_mutex.lock();
                file >> key;
                file >> value;
                my_tree->put(key, value);
                global_mutex.unlock();
            }else if (action == 'g') {
                global_mutex.lock_shared();
                file >> key;
                int query = NULL;
                if(my_tree->get(key, query)){
                    cout_mutex.lock();
                    *output<<query<<std::endl;
                    cout_mutex.unlock();
                }else{
                    cout_mutex.lock();
                    *output<<std::endl;
                    cout_mutex.unlock();
                }
                global_mutex.unlock_shared();
            }else if (action == 'd') {
                global_mutex.lock();
                file >> key;
                my_tree->del(key);
                global_mutex.unlock();
            }else if (action == 'r'){
                global_mutex.lock_shared();
                int low, high;
                file >> low;
                file >> high;
                std::vector<KVpair> res = my_tree->range(low, high);
                cout_mutex.lock();
                for(int i = 0; i < res.size(); i++){
                    *output<< res.at(i).key << ":"<<res.at(i).value<<" ";
                }
                *output<<std::endl;
                cout_mutex.unlock();
                global_mutex.unlock_shared();
            }else{
                *output<< "Error";
            }
        }
        file.close();
    }
}

void execute_workload_single_output1(Tree* my_tree, std::ofstream* output){
    std::ifstream file(workload2);
    char action;
    int key, value;
    if (file.is_open()) {
        while (!file.eof()) {
            file >> action;
            if (action == 'p') {
                global_mutex.lock();
                file >> key;
                file >> value;
                my_tree->put(key, value);
                global_mutex.unlock();
            }else if (action == 'g') {
                global_mutex.lock_shared();
                file >> key;
                int query = NULL;
                if(my_tree->get(key, query)){
                    cout_mutex.lock();
                    *output<<query<<std::endl;
                    cout_mutex.unlock();
                }else{
                    cout_mutex.lock();
                    *output<<std::endl;
                    cout_mutex.unlock();
                }
                global_mutex.unlock_shared();
            }else if (action == 'd') {
                global_mutex.lock();
                file >> key;
                my_tree->del(key);
                global_mutex.unlock();
            }else if (action == 'r'){
                global_mutex.lock_shared();
                int low, high;
                file >> low;
                file >> high;
                std::vector<KVpair> res = my_tree->range(low, high);
                cout_mutex.lock();
                for(int i = 0; i < res.size(); i++){
                    *output<< res.at(i).key << ":"<<res.at(i).value<<" ";
                }
                *output<<std::endl;
                cout_mutex.unlock();
                global_mutex.unlock_shared();
            }else{
                *output<< "Error";
            }
        }
        file.close();
    }
}


void multi_thread_test(bool single){
    Tree* my_tree = new Tree();
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    if(single){
        std::ofstream output("out_range_multi_single.txt");
        std::thread thread1(execute_workload_single_output, my_tree, &output);
        std::thread thread2(execute_workload_single_output, my_tree, &output);
        
        thread1.join();
        thread2.join();
        
        output.close();
    }else{
        std::thread thread1(execute_workload, my_tree);
        std::thread thread2(execute_workload1, my_tree);
        
        thread1.join();
        thread2.join();
    }

    
    high_resolution_clock::time_point t2 = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>( t2 - t1 ).count();
    std::cout << "-----------------------------------------------------" << std::endl;
    std::cout << "The elapsed time is " << duration << " microseconds" << std::endl;
    
}







int main(int argc, const char * argv[]) {
    //merge_test_file();
    //read_file("run_1_0", 3);
    //read_file("run_1_1", 3);
    //bloomfilter_test();
    //create_file();
    //main_test();
    //tree_test();
    //range_test();
    multi_thread_test(false);
    //concurrent_read();
    //concurrent_write(true);
    //read_then_write();
    //write_then_read(false);
}





