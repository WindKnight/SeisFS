/*
 * seiscache_head_reader.cpp
 *
 *  Created on: May 31, 2018
 *      Author: wyd
 */

#include "seiscache_head_reader.h"
#include "seiscache_error.h"
#include "util/seisfs_util_log.h"

namespace seisfs {

namespace cache {

HeadReader::HeadReader(SharedPtr<file::HeadReader> f_head_reader, StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
		ReaderBase(real_store_type, meta_manager), f_head_reader_(f_head_reader){
}
HeadReader::~HeadReader() {
	if(!is_closed_) {
		Close();
	}
}

bool HeadReader::SetHeadFilter(SharedPtr<HeadFilter> head_filter) {
	ASSERT_CLOSED(false);
	head_filter_ = head_filter;
	if(NULL != head_filter) {
		return f_head_reader_->SetHeadFilter(*head_filter_);
	} else {
		return true;
	}
}

bool HeadReader::SetRowFilter(SharedPtr<RowFilter> row_filter) {
	ASSERT_CLOSED(false);
	row_filter_ = row_filter;
	if(NULL != row_filter) {
		return f_head_reader_->SetRowFilter(*row_filter_);
	} else {
		return true;
	}
}

int HeadReader::GetHeadSize() {
	ASSERT_CLOSED(-1);
	return f_head_reader_->GetHeadSize();
} //return length of one head after filter, in bytes

int64_t HeadReader::GetTraceNum() {
	ASSERT_CLOSED(-1);
	return f_head_reader_->GetTraceNum();
}

int64_t HeadReader::Tell() {
	ASSERT_CLOSED(-1);
	return f_head_reader_->Tell();
}

bool HeadReader::Seek(uint64_t trace_idx) {
	ASSERT_CLOSED(false);
	return f_head_reader_->Seek(trace_idx);
}

bool HeadReader::HasNext() {
	ASSERT_CLOSED(false);
	return f_head_reader_->HasNext();
}

bool HeadReader::Close() {
	if(is_closed_) {
		return true;
	} else {
		if(!f_head_reader_->Close()) {
			sc_errno = SCCombineErrno(SC_ERR_CLOSE_FILE, errno);
			return false;
		}
		if(!ReaderBase::Close())
			return false;
		is_closed_ = true;
		return true;
	}
}

bool HeadReader::Get(uint64_t trace_idx, void *head) {
	ASSERT_CLOSED(false);

	if(!f_head_reader_->Get(trace_idx, head)) {
		sc_errno = SCCombineErrno(SC_ERR_FRD, errno);
		return false;
	}
	return true;
}

bool HeadReader::Next(void* head) {
	ASSERT_CLOSED(false);

	if(!f_head_reader_->Next(head)) {
		sc_errno = SCCombineErrno(SC_ERR_FRD, errno);
		return false;
	}
	return true;
}

HeadGather *HeadReader::NextGather() {
	ASSERT_CLOSED(NULL);
	if(NULL == order_) {
		return NULL;
	}
	HeadGather *head_gather = new HeadGather(); //TODO support gather when we have order
	return head_gather;
}

bool HeadReader::SeekToGather(int gather_idx) {
	ASSERT_CLOSED(false);
	if(NULL == order_) {
		return false;
	}

	return true;//TODO
}



bool HeadGather::Next(void *head) {

}
bool HeadGather::GetAll(void *head) {

}

}

}


