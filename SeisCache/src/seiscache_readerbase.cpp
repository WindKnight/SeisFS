/*
 * seiscache_readerbase.cpp
 *
 *  Created on: May 31, 2018
 *      Author: wyd
 */


#include "seiscache_readerbase.h"
#include "util/seisfs_util_log.h"

namespace seisfs {

namespace cache{

ReaderBase::ReaderBase (StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
	real_store_type_(real_store_type), meta_manager_(meta_manager){

	head_filter_ = NULL;
	row_filter_ = NULL;
	row_filter_by_key_ = NULL;
	order_ = NULL;

	is_closed_ = false;
}
ReaderBase::~ReaderBase() {

}

bool ReaderBase::SetHeadFilter(SharedPtr<HeadFilter> head_filter) {
	return false;
}

bool ReaderBase::SetRowFilter(SharedPtr<RowFilter> row_filter) {
	return false;
}

bool ReaderBase::Close() {
	if(!meta_manager_->RecordLastAccessed())
		return false;

	return true;
}


bool ReaderBase::SetRowFilter(SharedPtr<RowFilterByKey> row_filter_by_key) {
	row_filter_by_key_ = row_filter_by_key;
	return true;
}

bool ReaderBase::SetOrder(SharedPtr<Order> order) {
	order_ = order;
	return true;
}


GatherBase::GatherBase () {

}

GatherBase::~GatherBase() {

}

int64_t GatherBase::GetTraceNum() {

}

bool GatherBase::HasNext() {

}


}

}

