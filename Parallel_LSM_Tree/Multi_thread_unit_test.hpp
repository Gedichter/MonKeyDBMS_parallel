//
//  Multi_thread_unit_test.hpp
//  Parallel_LSM_Tree
//
//  Created by Shiyu Huang on 4/26/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#ifndef Multi_thread_unit_test_hpp
#define Multi_thread_unit_test_hpp

#include <stdio.h>
#include <thread>
#include <shared_mutex>
#include <chrono>
#include "Tree_thread_test.hpp"


bool get(Test_Tree* tree, int key, int id){
    int value;
    bool res = tree->get(key, value, id);
    return res;
}

void put(Test_Tree* tree,int id, bool wait){
    for(int i = 0; i < 5; i++){
        int key = i;
        int value = i+1;
        tree->put(key, value, id);
        if(wait) std::this_thread::sleep_for(std::chrono::microseconds(5));
    }

}

void concurrent_read(){
    Test_Tree* my_tree = new Test_Tree();
    //put some initial values in the tree
    for(int i = 0; i < 10; i++){
        my_tree->put(i, i, 0);
    }
    std::cout<<"-------setup phase ended-------"<<std::endl;
    std::thread thread1(get, my_tree, 2, 1);
    std::thread thread2(get, my_tree, 4, 2);
    
    thread1.join();
    thread2.join();
}

void concurrent_write(bool wait){
    Test_Tree* my_tree = new Test_Tree();
    std::thread thread1(put, my_tree, 1, wait);
    std::thread thread2(put, my_tree, 2, wait);
    
    thread1.join();
    thread2.join();
}

void read_then_write(){
    Test_Tree* my_tree = new Test_Tree();
    for(int i = 0; i < 5; i++){
        my_tree->put(i, i, 0);
    }
    std::cout<<"-------setup phase ended-------"<<std::endl;
    std::thread thread1(get, my_tree, 4, 1);
    std::thread thread2(put, my_tree, 2, false);
    
    thread1.join();
    thread2.join();
}

void write_then_read(bool wait){
    Test_Tree* my_tree = new Test_Tree();
    for(int i = 0; i < 5; i++){
        my_tree->put(i, i, 0);
    }
    std::cout<<"-------setup phase ended-------"<<std::endl;
    std::thread thread2(put, my_tree, 2, wait);
    std::thread thread1(get, my_tree, 4, 1);    
    thread1.join();
    thread2.join();
}
#endif /* Multi_thread_unit_test_hpp */
