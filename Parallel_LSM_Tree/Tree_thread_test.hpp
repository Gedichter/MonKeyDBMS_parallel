//
//  Tree_thread_test.hpp
//  Parallel_LSM_Tree
//
//  Created by Shiyu Huang on 4/26/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#ifndef Tree_thread_test_hpp
#define Tree_thread_test_hpp

#include <stdio.h>
#include "LSM.hpp"
#include <vector>
#include <shared_mutex>
#include <mutex>


class Test_Tree{
    Buffer buffer;
    static std::shared_timed_mutex global_mutex;
    
public:
    std::vector<Layer> layers;
    Test_Tree();
    void flush();
    bool bufferFlush();
    bool layerFlush(Layer &low, Layer &high);
    void put(int key, int value, int id);
    bool get(int key, int& value, int id);
    void del(int key);
    std::vector<KVpair> range(int low, int high);
    
};

#endif /* Tree_thread_test_hpp */
