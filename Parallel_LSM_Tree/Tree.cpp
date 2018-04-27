//
//  Tree.cpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/22/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include "Tree.hpp"
#include "LSM.hpp"
#include <cmath>
#include <unordered_map>



Tree::Tree(){
    Layer layer;
    layer.set_rank(0);
    layers.push_back(layer);
    std::shared_timed_mutex* layer_mutex = new std::shared_timed_mutex;
    layers_mutexes.push_back(layer_mutex);
}

std::shared_timed_mutex Tree::buffer_mutex;
std::vector<std::shared_timed_mutex*> Tree::layers_mutexes;

/**
 flush buffer to the LSM tree
 
 @return when true, the first layer has reached its limit
 */
bool Tree::bufferFlush(){
    buffer.sort();
    layers_mutexes[0]->lock();
    bool return_value = layers[0].add_run_from_buffer(buffer);
    //layers_mutexes[0].unlock();
    layers_mutexes[0]->unlock();
    return return_value;
}

/**
 flush level in the LSM tree
 
 @param low layer to be flushed
 high layer to be flushed in
 @return when true, the high layer has reached its limit
 */
bool Tree::layerFlush(Layer &low, Layer &high){
    int num_pointers = 0;
    unsigned long size = 0;
    BloomFilter *bf = NULL;
    FencePointer *fp = NULL;
    std::string new_run = low.merge(size, bf, fp, num_pointers);
    return high.add_run(new_run, size, bf, fp, num_pointers);
};

void Tree::flush(){
    int level = 0;
    bool goOn = true;
    while(goOn && level + 1 < layers.size()){
//        layers_mutexes.at(level).lock();
//        layers_mutexes.at(level+1).lock();
        layers_mutexes.at(level)->lock();
        layers_mutexes.at(level+1)->lock();
        goOn = layerFlush(layers.at(level), layers.at(level+1));
//        layers_mutexes.at(level+1).unlock();
//        layers_mutexes.at(level).unlock();
        layers_mutexes.at(level+1)->unlock();
        layers_mutexes.at(level)->unlock();
        level += 1;
    }
    if(goOn){
        Layer layer;
        layer.set_rank(layers.size());
        std::shared_timed_mutex* layer_mutex = new std::shared_timed_mutex;
        layers_mutexes.push_back(layer_mutex);
//        layers_mutexes.at(level).lock();
//        //lock the layer before it is pushed into the layers to guarantee no invalid access
//        layers_mutexes.at(level+1).lock();
        layers_mutexes.at(level)->lock();
        //lock the layer before it is pushed into the layers to guarantee no invalid access
        layers_mutexes.at(level+1)->lock();
        layers.push_back(layer);
        layerFlush(layers.at(level), layers.at(level+1));
//        layers_mutexes.at(level+1).unlock();
//        layers_mutexes.at(level).unlock();
        layers_mutexes.at(level+1)->unlock();
        layers_mutexes.at(level)->unlock();

    }
}

bool Tree::get(int key, int& value){
    buffer_mutex.lock_shared();
    int get_result = buffer.get(key, value);
    buffer_mutex.unlock_shared();
    switch (get_result) {
        case 1:
            return true;
        case -1:
            return false;
        default:
            for(int i = 0; i < layers.size(); i++){
//                layers_mutexes.at(i).lock_shared();
                layers_mutexes.at(i)->lock_shared();
                int layer_result = layers.at(i).get(key, value);
//                layers_mutexes.at(i).unlock_shared();
                layers_mutexes.at(i)->unlock_shared();
                switch (layer_result) {
                    case 1:
                        return true;
                    case -1:
                        return false;
                    default:
                        break;
                }
            }
    }
    return false;
};


/**
 return the all the key value pairs within the range
 @params low : include
 high : not include
 return vector of the key-value pair
 */
std::vector<KVpair> Tree::range(int low, int high){
    std::unordered_map<int, KVpair> result_buffer;
    buffer_mutex.lock_shared();
    buffer.range(low, high, result_buffer);
    buffer_mutex.unlock_shared();
    for(int i = 0; i < layers.size(); i++){
        //layers_mutexes.at(i).lock_shared();
        layers_mutexes.at(i)->lock_shared();
        layers.at(i).range(low, high, result_buffer);
        //layers_mutexes.at(i).unlock_shared();
        layers_mutexes.at(i)->unlock_shared();
    }
    std::vector<KVpair> result;
    for (auto const& x : result_buffer)
    {
        KVpair kv = x.second;
        if(!kv.del){
            result.push_back(kv);
        }
    }
    return result;
};


void Tree::put(int key, int value){
    buffer_mutex.lock();
    bool full = buffer.put(key, value);
    if(full){
        bool layer0_full = bufferFlush();
        buffer_mutex.unlock();
        if(layer0_full){
            flush();
        }
    }else{
        buffer_mutex.unlock();
    }
};

void Tree::del(int key){
    buffer_mutex.lock();
    bool full = buffer.del(key);
    if(full){
        bool layer0_full = bufferFlush();
        buffer_mutex.unlock();
        if(layer0_full){
            flush();
        }
    }else{
        buffer_mutex.unlock();
    }
};

