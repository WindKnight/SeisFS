/*
 * seiscache_reader.cpp
 *
 *  Created on: May 31, 2018
 *      Author: wyd
 */

#include "seiscache_reader.h"
#include "seiscache_error.h"


namespace seisfs {

namespace cache {

Reader::Reader(SharedPtr<file::Reader> f_reader, StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
		ReaderBase(real_store_type, meta_manager), f_reader_(f_reader) {

}
Reader::~Reader() {
	if(!is_closed_) {
		Close();
	}
}

bool Reader::SetHeadFilter(SharedPtr<HeadFilter> head_filter) {
	ASSERT_CLOSED(false);
	head_filter_ = head_filter;
	if(NULL != head_filter) {
		return f_reader_->SetHeadFilter(*head_filter_);
	} else {
		return true;
	}
}

bool Reader::SetRowFilter(SharedPtr<RowFilter> row_filter) {
	ASSERT_CLOSED(false);
	row_filter_ = row_filter;
	if(NULL != row_filter) {
		return f_reader_->SetRowFilter(*row_filter_);
	} else {
		return true;
	}
}


int64_t Reader::GetTraceNum() {
	ASSERT_CLOSED(-1);
	return f_reader_->GetTraceNum();
}

int64_t Reader::Tell() {
	ASSERT_CLOSED(-1);
	return f_reader_->Tell();
}

bool Reader::Seek(uint64_t trace_idx) {
	ASSERT_CLOSED(false);
	return f_reader_->Seek(trace_idx);
}

bool Reader::HasNext() {
	ASSERT_CLOSED(false);
	return f_reader_->HasNext();
}

bool Reader::Close() {
	if(is_closed_) {
		return true;
	} else {
		if(!f_reader_->Close()) {
			sc_errno = SCCombineErrno(SC_ERR_CLOSE_FILE, errno);
			return false;
		}
		if(!ReaderBase::Close())
			return false;
		is_closed_ = true;
		return true;
	}
}

int Reader::GetHeadSize() {
	ASSERT_CLOSED(-1);
	return f_reader_->GetHeadSize();

}//return length of one head after filter, in bytes
int Reader::GetTraceSize() {
	ASSERT_CLOSED(-1);
	return f_reader_->GetTraceSize();

}//return length of one trace, in bytes

/*
 * Get does not influence the cursor of Next
 */
bool Reader::Get(uint64_t trace_idx, void *head, void* trace) {
	ASSERT_CLOSED(false);
	if(!f_reader_->Get(trace_idx, head, trace)) {
		sc_errno = SCCombineErrno(SC_ERR_FRD, errno);
		return false;
	}
	return true;
}

bool Reader::Next(void* head, void* trace) {
	ASSERT_CLOSED(false);
	if(!f_reader_->Next(head, trace)) {
		sc_errno = SCCombineErrno(SC_ERR_FRD, errno);
		return false;
	}
	return true;
}

/*
 * get next trace gather defined by order.
 */
Gather *Reader::NextGather() {
	ASSERT_CLOSED(NULL);
	if(NULL == order_) {
		return NULL;
	}
	Gather *gather = new Gather(); //TODO
	return gather;
}

bool Reader::SeekToGather(int gather_id) {
	ASSERT_CLOSED(false);
	if(NULL == order_) {
		return false;
	}

	return true;//TODO
}

Gather::Gather() {

}
Gather::~Gather() {

}

bool Gather::Next(void *head, void *trace) {

}
bool Gather::GetAll(void *head, void *trace) {

}

}

}

