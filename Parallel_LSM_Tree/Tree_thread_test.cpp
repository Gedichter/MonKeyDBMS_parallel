//
//  Tree_thread_test.cpp
//  Parallel_LSM_Tree
//
//  Created by Shiyu Huang on 4/26/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include "Tree_thread_test.hpp"
#include "LSM.hpp"
#include <cmath>
#include <unordered_map>


Test_Tree::Test_Tree(){
    Layer layer;
    layer.set_rank(0);
    layers.push_back(layer);
}

std::shared_timed_mutex Test_Tree::global_mutex;

/**
 flush buffer to the LSM tree
 
 @return when true, the first layer has reached its limit
 */
bool Test_Tree::bufferFlush(){
    buffer.sort();
    return layers[0].add_run_from_buffer(buffer);
}

/**
 flush level in the LSM tree
 
 @param low layer to be flushed
 high layer to be flushed in
 @return when true, the high layer has reached its limit
 */
bool Test_Tree::layerFlush(Layer &low, Layer &high){
    int num_pointers = 0;
    unsigned long size = 0;
    BloomFilter *bf = NULL;
    FencePointer *fp = NULL;
    std::string new_run = low.merge(size, bf, fp, num_pointers);
    return high.add_run(new_run, size, bf, fp, num_pointers);
};

void Test_Tree::flush(){
    if(bufferFlush()){
        int level = 0;
        bool goOn = true;
        while(goOn && level + 1 < layers.size()){
            goOn = layerFlush(layers.at(level), layers.at(level+1));
            level += 1;
        }
        if(goOn){
            Layer layer;
            layer.set_rank(layers.size());
            layers.push_back(layer);
            layerFlush(layers.at(level), layers.at(level+1));
        }
    }
    
}

void Test_Tree::put(int key, int value, int id){
    global_mutex.lock();
    if(buffer.put(key, value)){
        flush();
    }
    std::cout<<"thread " << id << " successfully writes "<< key << "->" << value <<std::endl;
    global_mutex.unlock();
};

bool Test_Tree::get(int key, int& value, int id){
    global_mutex.lock_shared();
    std::cout<<"thread " << id << " queries key "<<key<<std::endl;
    switch (buffer.get(key, value)) {
        case 1:
            global_mutex.unlock_shared();
            std::cout<<"The value is " << value<<std::endl;
            return true;
        case -1:
            global_mutex.unlock_shared();
            return false;
        default:
            for(int i = 0; i < layers.size(); i++){
                switch (layers.at(i).get(key, value)) {
                    case 1:
                        std::cout<<"The value is " << value<<std::endl;
                        global_mutex.unlock_shared();
                        return true;
                    case -1:
                        global_mutex.unlock_shared();
                        return false;
                    default:
                        break;
                }
            }
    }
    global_mutex.unlock_shared();
    return false;
};


/**
 return the all the key value pairs within the range
 @params low : include
 high : not include
 return vector of the key-value pair
 */
std::vector<KVpair> Test_Tree::range(int low, int high){
    global_mutex.lock_shared();
    std::unordered_map<int, KVpair> result_buffer;
    buffer.range(low, high, result_buffer);
    for(int i = 0; i < layers.size(); i++){
        layers.at(i).range(low, high, result_buffer);
    }
    std::vector<KVpair> result;
    for (auto const& x : result_buffer)
    {
        KVpair kv = x.second;
        if(!kv.del){
            result.push_back(kv);
        }
    }
    global_mutex.unlock_shared();
    return result;
};

void Test_Tree::del(int key){
    global_mutex.lock();
    if(buffer.del(key)){
        flush();
    }
    global_mutex.unlock();
};
