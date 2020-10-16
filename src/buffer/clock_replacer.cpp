//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <iostream>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    num_page = num_pages;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> guard(mtx);
    if(lst.size()==0){frame_id = nullptr; return false;}
    *frame_id = lst[0];
    hash[*frame_id]=false;
    lst.erase(lst.begin());
    return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(mtx);
    if(find(lst.begin(), lst.end(), frame_id)!= lst.end()){
        ref = distance(lst.begin(), find(lst.begin(), lst.end(), frame_id));
        hash[frame_id]=false;
        lst.erase(lst.begin()+ref);
        if(ref==lst.size())ref=0;

    }
    // std::cout<<"Pin"<<ref;

}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(mtx);
    if(find(lst.begin(), lst.end(), frame_id)==lst.end() && lst.size()<num_page){
        lst.push_back(frame_id);
        ref = distance(lst.begin(), find(lst.begin(), lst.end(), frame_id));
        hash[frame_id]= true;
        // std::cout<<"---A--"<<ref;
    }
    else if(find(lst.begin(), lst.end(), frame_id)==lst.end()){
        temp = lst[ref];
        while(hash[temp]){
            hash[temp]=false;
            if(ref<num_page-1){ref++;}
            else{ref=0;}
            temp = lst[ref];
        }
        hash[frame_id]=true;
        lst[ref]=frame_id;
        ref = distance(lst.begin(), find(lst.begin(), lst.end(), frame_id));
        // std::cout<<"---B--"<<ref;
    }
    else{
        if(hash[frame_id]){
            ref = distance(lst.begin(), find(lst.begin(), lst.end(), frame_id));
            // std::cout<<"---C---"<<ref;
            }
        else{
            hash[frame_id]=true;
            ref = distance(lst.begin(), find(lst.begin(), lst.end(), frame_id));
            // std::cout<<"---D---"<<ref;
        }
        

    }
    

}

size_t ClockReplacer::Size() { 
    std::lock_guard<std::mutex> guard(mtx);
    // std::cout<<"size:"<<lst.size();
    return lst.size();
}

}  // namespace bustub
