/*
 * seisfs_head_filter.cpp
 *
 *  Created on: Apr 5, 2018
 *      Author: wyd
 */

#include "seisfs_head_filter.h"

namespace seisfs {


HeadFilter::HeadFilter() {

}
HeadFilter::HeadFilter(const HeadFilter& head_filter) {
	key_arr_ = head_filter.key_arr_;
	key_set_ = head_filter.key_set_;
}
HeadFilter::~HeadFilter() {

}

void HeadFilter::AddKey(int key_id) {
    if (key_set_.count(key_id) == 0) {
    	key_arr_.push_back(key_id);
        key_set_.insert(key_id);
    }
}

int HeadFilter::NumKeys() const {
	return key_arr_.size();
}

const std::list<int>&  HeadFilter::GetKeyList() const{
	return key_arr_;
}

}


