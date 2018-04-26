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
#include "Tree.hpp"

std::shared_timed_mutex rwlock;

bool get(Tree* tree, int key, int id){
    int value;
    rwlock.lock_shared();
    bool res = tree->get(key, value);
    std::cout<<"thread " << id << " get value "<<value<<std::endl;
    rwlock.unlock_shared();
    return res;
}

void put(Tree* tree,int id, bool wait){
    for(int i = 0; i < 5; i++){
        int key = i;
        int value = i+1;
        rwlock.lock();
        tree->put(key, value);
        std::cout<<"thread " << id << " successfully writes "<< key << "->" << value <<std::endl;
        rwlock.unlock();
        if(wait) std::this_thread::sleep_for(std::chrono::microseconds(5));
    }

}

void concurrent_read(){
    Tree* my_tree = new Tree();
    //put some initial values in the tree
    for(int i = 0; i < 10; i++){
        my_tree->put(i, i);
    }
    std::thread thread1(get, my_tree, 2, 1);
    std::thread thread2(get, my_tree, 4, 2);
    
    thread1.join();
    thread2.join();
}

void concurrent_write(bool wait){
    Tree* my_tree = new Tree();
    std::thread thread1(put, my_tree, 1, wait);
    std::thread thread2(put, my_tree, 2, wait);
    
    thread1.join();
    thread2.join();
}

void read_then_write(){
    Tree* my_tree = new Tree();
    for(int i = 0; i < 5; i++){
        my_tree->put(i, i);
    }
    std::thread thread1(get, my_tree, 4, 1);
    std::thread thread2(put, my_tree, 2, false);
    
    thread1.join();
    thread2.join();
}

void write_then_read(bool wait){
    Tree* my_tree = new Tree();
    for(int i = 0; i < 5; i++){
        my_tree->put(i, i);
    }
    std::thread thread2(put, my_tree, 2, wait);
    std::thread thread1(get, my_tree, 4, 1);    
    thread1.join();
    thread2.join();
}
#endif /* Multi_thread_unit_test_hpp */
