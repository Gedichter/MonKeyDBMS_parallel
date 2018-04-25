//
//  LSM.cpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/21/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include <iostream>
#include "LSM.hpp"
#include <vector>
#include <algorithm>
#include <assert.h>
#include <climits>
#include <fstream>
#include <cmath>


/**
 utility function
 */

bool compareKVpair(KVpair pair1, KVpair pair2){
    return pair1.key < pair2.key;
}

/*
 Create a bloom filter for the run
 @param run the array of the KVpairs in a run
 numBits the filter size in bits:m
 numHashes number of hash functions in a filter:k
 @return the pointer to the bloom filter
 */
BloomFilter* create_bloom_filter(KVpair* run, unsigned long int numEntries, double falPosRate){
    BloomFilter* filter = new BloomFilter(numEntries, falPosRate);
    for(int i = 0; i < numEntries; i++){
        filter->add(run[i].key);
    }
    return filter;
};

/*
 Create an array of fence pointer for the run
 Called only when the size of the run is greater than parameters::KVPAIRPERPAGE
 @param run is the array of the KVpairs in a run
 size is the length of the run
 num_pointers stores the number of fence pointers in the array
 @return the pointer to the array
 */
FencePointer* create_fence_pointer(KVpair* run, unsigned long int size, int& num_pointers){
    num_pointers = (int)ceil((double)size/(double)parameters::KVPAIRPERPAGE);
    FencePointer* fparray = new FencePointer[num_pointers];
    for(int i = 0; i < num_pointers; i++){
        fparray[i].min = run[i*parameters::KVPAIRPERPAGE].key;
        fparray[i].max = run[std::min((i+1)*parameters::KVPAIRPERPAGE-1, size-1)].key;
    }
    return fparray;
}

/** Buffer
 */


/**
 Put the value associated with the key in the buffer
 @param
 key the key to insert
 value the value to insert
 @return when true, the buffer has reached capacity
 */
bool Buffer::put(int key, int value){
    bool exist = false;
    for(int i = 0; i < size; i++){
        if(data[i].key == key){
            data[i].value = value;
            data[i].del = false;
            exist = true;
        }
    }
    if(!exist){
        data[size].key = key;
        data[size].value = value;
        data[size].del = false;
        size += 1;
        if(size >= parameters::BUFFER_CAPACITY){
            return true;
        }
    }
    return false;
};


/**
Get the value associated with the key in the buffer
 
 @param key the key to delete
 value: address to store the return value
 @return 1: found
 0: not found
 -1: (latest version)deleted, which means no need to go on searching
 */
int Buffer::get(int key, int& value){
    for(int i = size-1; i >= 0; i--){
        if(data[i].key == key){
            if(data[i].del){
                return -1;
            }else{
                value = data[i].value;
                return 1;
            }
        }
    }
    return 0;
};

/**
 Delete the value associated with the key in the buffer
 
 @param key the key to delete
 @return when true, the buffer has reached capacity
 */
bool Buffer::del(int key){
    for(int i = 0; i < size; i++){
        if(data[i].key == key){
            data[i].del = true;
            return false;
        }
    }
    //when not found in the buffer
    data[size].key = key;
    data[size].value = 0;
    data[size].del = true;
    size += 1;
    if(size >= parameters::BUFFER_CAPACITY) return true;
    return false;
};

void Buffer::range(int low, int high, std::unordered_map<int, KVpair>& res){
    for(int i = 0; i < size; i++){
        int key = data[i].key;
        if(key < high && key >= low){
            res[key] = data[i];
        }
    }
}

void Buffer::sort(){
    std::sort(data, data+parameters::BUFFER_CAPACITY, compareKVpair);
};

/**
 Layer
 */


Layer::Layer(){
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        filters[i] = NULL;
    }
}


void Layer::set_rank(int r){
    rank = r;
}

/**
 Add element in the buffer to the first level of the LSM tree
 
 @param buffer the buffer
 @return when true, the first layer has reached its limit
 */
bool Layer::add_run_from_buffer(Buffer &buffer){
    //Bloom filter
    filters[current_run] = create_bloom_filter(buffer.data, parameters::BUFFER_CAPACITY, parameters::FPRATE0);
    //Fence pointer
    if(parameters::BUFFER_CAPACITY > parameters::KVPAIRPERPAGE){
        int numPointers = 0;
        pointers[current_run] = create_fence_pointer(buffer.data, parameters::BUFFER_CAPACITY, numPointers);
        pointer_size[current_run] = numPointers;
    }
    //write to file
    std::string name = get_name(current_run);
    std::ofstream run(name, std::ios::binary);
    runs[current_run] = name;
    run.write((char*)buffer.data, parameters::BUFFER_CAPACITY*sizeof(KVpair));
    run_size[current_run] = parameters::BUFFER_CAPACITY;
    current_run++;
    run.close();
    //TODO: change the setter on buffer
    buffer.size = 0;
    return current_run == parameters::NUM_RUNS;
};


/**
 Reset the layer, free memory, delete file
 */
void Layer::reset(){
    current_run = 0;
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        run_size[i] = 0;
        pointer_size[i] = 0;
        delete filters[i];
        if(pointers[i] != NULL){
            delete [] pointers[i];
            pointers[i] = NULL;
        }
        filters[i] = NULL;
        runs[i].clear();
        std::string name = get_name(i);
        if(remove(name.c_str()) != 0){
            std::cout<<"Error deleting the file"<<std::endl;
        };
    }
};

std::string Layer::get_name(int nthRun){
    return "run_" + std::to_string(rank) + "_" + std::to_string(nthRun);
}

/**
 Merge all runs to one run in this level
 NOTE: Can't use heap to do the merge sort because we need to maintain the order of runs to know
 which are the newest values
 Use temp vector to store the merged result then write to file: minimize number of I/O
 @param size stores the size of the resulting run
 @return the name of the file of the new run
 */
std::string Layer::merge(unsigned long &size, BloomFilter*& bf, FencePointer*& fp, int &num_pointers){
    //read files and set index
    KVpair *read_runs[parameters::NUM_RUNS];
    int* indexes = new int[parameters::NUM_RUNS];
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        indexes[i] = 0;
        std::ifstream inStream(get_name(i), std::ios::binary);
        read_runs[i] = new KVpair[run_size[i]];
        inStream.read((char*)read_runs[i], run_size[i]*sizeof(KVpair));
        inStream.close();
    }
    //perform merge
    std::vector<KVpair> run_buffer;
    int ct = parameters::NUM_RUNS; //the count of active arrays
    int min;
    while(ct > 0){
        std::vector<int> min_indexes;
        min = INT_MAX;
        //there is no duplicate inside each run, scan from the old runs to the new runs
        for(int i = 0; i < parameters::NUM_RUNS; i++){
            if(indexes[i] >= 0){
                if(read_runs[i][indexes[i]].key < min){
                    min = read_runs[i][indexes[i]].key;
                    min_indexes.clear();
                    min_indexes.push_back(i);
                }else if (read_runs[i][indexes[i]].key == min){
                    min_indexes.push_back(i);
                }
            }
        }
        int min_index = min_indexes.back();
        run_buffer.push_back(read_runs[min_index][indexes[min_index]]);
        for(int i = 0; i < min_indexes.size(); i++){
            int cur_index = min_indexes.at(i);
            indexes[cur_index] += 1;
            if(indexes[cur_index] >= run_size[cur_index]){
                //set index to -1 when the the last element of the array is used
                indexes[cur_index] = -1;
                ct -= 1;
            }
        }
    }
    //free space for intermediate storage
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        delete [] read_runs[i];
    }
    //set the new size, create array
    size = run_buffer.size();
    KVpair* new_run = new KVpair[size];
    //write to file
    std::string name = "run_" + std::to_string(rank) + "_temp";
    std::ofstream new_file(name, std::ios::binary);
    std::copy(run_buffer.begin(), run_buffer.end(), new_run);
    new_file.write((char*)new_run, size*sizeof(KVpair));
    new_file.close();
    //create bloom filter TODO: change to create_bloom_filter function!!
    //Here use the formula from the paper to calculate the fp rate for each level
    double fprate = parameters::FPRATE0*pow(parameters::SIZE_RATIO, rank);
    if(rank < parameters::LEVELWITHBF-1){
        bf = new BloomFilter(size, fprate);
        for(int i = 0; i < size; i++){
            bf->add(new_run[i].key);
        }
    }
    //create fence pointer
    if(size > parameters::KVPAIRPERPAGE){
        fp = create_fence_pointer(new_run, size, num_pointers);
    }
    //reset the layer, free the dynamic memory
    reset();
    delete[] indexes;
    delete [] new_run;
    
    return name;
};

/**
 Merge all runs to one run in this level
 NOTE: Can't use heap to do the merge sort because we need to maintain the order of runs to know
 which are the newest values
 Use temp vector to store the merged result then write to file: minimize number of I/O
 @param size stores the size of the resulting run
 @return the name of the file of the new run
 */
std::string Layer::pagewise_merge(unsigned long &new_run_size, BloomFilter*& bf, FencePointer*& fp, int &num_pointers){
    //read files and set index
    KVpair read_runs[parameters::NUM_RUNS][parameters::KVPAIRPERPAGE];
    int read_runs_length[parameters::NUM_RUNS] = {0};
    int current_read_length[parameters::NUM_RUNS] = {0};
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        //TODO: change to an array of open files
        std::ifstream inStream(get_name(i), std::ios::binary);
        int read_length = std::min(parameters::KVPAIRPERPAGE, run_size[i] - read_runs_length[i]);
        current_read_length[i] = read_length;
        inStream.read((char*)read_runs[i], read_length*sizeof(KVpair));
        read_runs_length[i] += read_length;
        inStream.close();
    }
    
    //set up the file to write
    std::string name = "run_" + std::to_string(rank) + "_temp";
    std::ofstream new_file(name, std::ios::binary);
    
    //set up bloom filter and fence pointer for the new run
    unsigned long size_ceiling = 0;
    for(int i = 0; i < parameters::NUM_RUNS; i++) size_ceiling += run_size[i];
    double fprate = parameters::FPRATE0*pow(parameters::SIZE_RATIO, rank);
    if(rank < parameters::LEVELWITHBF-1) bf = new BloomFilter(size_ceiling, fprate);
    std::vector<FencePointer> Fence_buffer;
    
    //perform merge
    int current_positions[parameters::NUM_RUNS] = {0}; //current position in the page
    KVpair merge_buffer[parameters::KVPAIRPERPAGE];
    int index_merge_buffer = 0;
    int ct = parameters::NUM_RUNS; //the count of active arrays
    int min;
    unsigned long new_run_count = 0;
    while(ct > 0){
        std::vector<int> indexes_min_run;
        min = INT_MAX;
        //there is no duplicate inside each run, scan from the old runs to the new runs
        for(int i = 0; i < parameters::NUM_RUNS; i++){
            if(current_positions[i] >= 0){
                if(read_runs[i][current_positions[i]].key < min){
                    min = read_runs[i][current_positions[i]].key;
                    indexes_min_run.clear();
                    indexes_min_run.push_back(i);
                }else if (read_runs[i][current_positions[i]].key == min){
                    indexes_min_run.push_back(i);
                }
            }
        }
        int min_index = indexes_min_run.back();
        //write to merge buffer
        merge_buffer[index_merge_buffer] = read_runs[min_index][current_positions[min_index]];
        if(bf != NULL) bf->add(merge_buffer[index_merge_buffer].key);
        index_merge_buffer += 1;
        new_run_count += 1;
        if(index_merge_buffer == parameters::KVPAIRPERPAGE){
            //merge buffer is full, write to file and reset the merge buffer
            FencePointer fp_temp;
            fp_temp.min = merge_buffer[0].key;
            fp_temp.max = merge_buffer[index_merge_buffer-1].key;
            Fence_buffer.push_back(fp_temp);
            new_file.write((char*)merge_buffer, index_merge_buffer*sizeof(KVpair));
            index_merge_buffer = 0;
        }
        for(int i = 0; i < indexes_min_run.size(); i++){
            int cur_index = indexes_min_run.at(i);
            current_positions[cur_index] += 1;
            if(current_positions[cur_index] >= current_read_length[cur_index]){
                //Current page is used up for this page
                //set index to -1 when the the last element of the array is used
                if(read_runs_length[cur_index] >= run_size[cur_index]){
                    current_positions[cur_index] = -1;
                    ct -= 1;
                }else{
                    current_positions[cur_index] = 0;
                    std::ifstream inStream(get_name(cur_index), std::ios::binary);
                    inStream.seekg(read_runs_length[cur_index]*sizeof(KVpair));
                    int read_length = std::min(parameters::KVPAIRPERPAGE, run_size[cur_index] - read_runs_length[cur_index]);
                    inStream.read((char*)read_runs[cur_index], read_length*sizeof(KVpair));
                    current_read_length[cur_index] = read_length;
                    read_runs_length[cur_index] += read_length;
                    inStream.close();
                }
            }
        }
    }
    //write the remaining part in the merge_buffer to the result
    FencePointer fp_temp;
    fp_temp.min = merge_buffer[0].key;
    fp_temp.max = merge_buffer[index_merge_buffer-1].key;
    Fence_buffer.push_back(fp_temp);
    new_file.write((char*)merge_buffer, index_merge_buffer*sizeof(KVpair));
    new_file.close();
    
    //create fence pointer
    num_pointers = Fence_buffer.size();
    new_run_size = new_run_count;
    FencePointer* fparray = new FencePointer[num_pointers];
    std::copy(Fence_buffer.begin(), Fence_buffer.end(), fparray);
    fp = fparray;
    
    //reset the layer, free the dynamic memory
    reset();
    return name;
};

/**
 Add new run from the previous level of the LSM tree
 
 @param run the pointer to the new run
 size the size of the new run
 @return when true, the layer has reached its limit
 */
bool Layer::add_run(std::string run, unsigned long size, BloomFilter* bf, FencePointer* fp, int num_pointers){
    std::string newName = get_name(current_run);
    if(rename(run.c_str(), newName.c_str()) != 0){
        std::cout << "rename failed"<<std::endl;
    };
    runs[current_run] = newName;
    run_size[current_run] = size;
    filters[current_run] = bf;
    pointers[current_run] = fp;
    pointer_size[current_run] = num_pointers;
    current_run += 1;
    return current_run >= parameters::NUM_RUNS;
}


/**
Check if the key is in the run
 
 @param key The key to check
value the value associated with the key
 index: the index number of the run in the level
 @return 1:found, 0:not found, -1:deleted
 */
int Layer::check_run(int key, int& value, int index){
    //indexes for the fence pointer
    unsigned long int offset = 0;
    bool valid = false;
    //check the fence pointer
    if(pointers[index] != NULL){
        for(int i = 0; i < pointer_size[index]; i++){
            if(key>= pointers[index][i].min && key <= pointers[index][i].max){
                offset = i*parameters::KVPAIRPERPAGE;
                valid = true;
                break;
            }
        }
        if(!valid) return 0;
    }
    //read the needed page from the file
    unsigned long read_size = run_size[index];
    if(valid){
        read_size = std::min(parameters::KVPAIRPERPAGE, (run_size[index]-offset));
    }
    KVpair* curRun = new KVpair[read_size];
    std::ifstream inStream(get_name(index), std::ios::binary);
    inStream.seekg(offset*sizeof(KVpair));
    inStream.read((char *)curRun, read_size*sizeof(KVpair));
    inStream.close();
    //TODO: change to binary search
    for(int j = 0; j < read_size; j++){
        if(curRun[j].key == key){
            if(curRun[j].del){
                return -1;
            }else{
                value = curRun[j].value;
                return 1;
            }
        }
    }
    delete[] curRun;
    return 0;
}

/**
 a basic implementation of get
 @return 1: found
 0: not found
 -1: (latest version)deleted, which means no need to go on searching
 */
int Layer::get(int key, int& value){
    for(int i = current_run-1; i >= 0; i--){
        if(rank >= parameters::LEVELWITHBF){
            int c = check_run(key, value, i);
            if(c!=0) return c;
        }else{
            if(filters[i]->possiblyContains(key)){
                int c = check_run(key, value, i);
                if(c!=0) return c;
            }
        }
    }
    return 0;
};

/**
 Do range query on the whole run
 */
void Layer::range(int low, int high, std::unordered_map<int, KVpair>& range_buffer){
    for(int i = current_run-1; i >= 0; i--){
        range_run(low, high, range_buffer, i);
    }
};

/**
 Do range query on a run
 */
void Layer::range_run(int low, int high, std::unordered_map<int, KVpair>& range_buffer, int index){
    std::vector<int> offsets;
    std::vector<unsigned long> read_sizes;
    
    //check the fence pointer
    if(pointers[index] != NULL){
        for(int i = 0; i < pointer_size[index]; i++){
            if(!(high <= pointers[index][i].min || low > pointers[index][i].max)){
                offsets.push_back(i*parameters::KVPAIRPERPAGE);
                read_sizes.push_back(std::min(parameters::KVPAIRPERPAGE, (run_size[index]-offsets.back())));
            }
        }
    }
    if(offsets.empty()){
        offsets.push_back(0);
        read_sizes.push_back(run_size[index]);
    }
    
    //read the needed page from the file
    for(int i = 0; i < offsets.size(); i++){
        int offset = offsets.at(i);
        unsigned long read_size = read_sizes.at(i);
        KVpair* curRun = new KVpair[read_size];
        std::ifstream inStream(get_name(index), std::ios::binary);
        inStream.seekg(offset*sizeof(KVpair));
        inStream.read((char *)curRun, read_size*sizeof(KVpair));
        inStream.close();
        //TODO: change to binary search
        for(int j = 0; j < read_size; j++){
            int key = curRun[j].key;
            if(key < high && key >= low && range_buffer.find(key) == range_buffer.end()){
                range_buffer[key] = curRun[j];
            }
        }
        delete[] curRun;
    }

}















//struct MinHeapNode
//{
//    //values of the
//    int key;
//    int value;
//    bool del;
//    int i; // index of the array from which the element is taken
//    int j; // index of the next element to be picked from array
//};
//
//static bool heapComp(MinHeapNode *a, MinHeapNode *b) {
//    return a->value > b->value;
//}
//
//MinHeapNode *createNode(int key, int value, bool del, int i, int j){
//    MinHeapNode *node = new MinHeapNode;
//    node->key = key;
//    node->value = value;
//    node->del = del;
//    node->i = i;
//    node->j = j;
//    return node;
//}

