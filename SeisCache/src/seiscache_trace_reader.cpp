/*
 * seiscache_trace_reader.cpp
 *
 *  Created on: May 31, 2018
 *      Author: wyd
 */


#include "seiscache_trace_reader.h"
#include "seiscache_error.h"

namespace seisfs {

namespace cache{

TraceReader::TraceReader(SharedPtr<file::TraceReader> f_trace_reader, StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
		ReaderBase(real_store_type, meta_manager), f_trace_reader_(f_trace_reader) {

}

TraceReader::~TraceReader() {
	if(!is_closed_) {
		Close();
	}
}

bool TraceReader::SetRowFilter(SharedPtr<RowFilter> row_filter) {
	ASSERT_CLOSED(false);
	row_filter_ = row_filter;
	if(NULL != row_filter) {
		return f_trace_reader_->SetRowFilter(*row_filter_);
	} else {
		return true;
	}
}

int64_t TraceReader::GetTraceNum() {
	ASSERT_CLOSED(-1);
	return f_trace_reader_->GetTraceNum();
}

int64_t TraceReader::Tell() {
	ASSERT_CLOSED(-1);
	return f_trace_reader_->Tell();
}

bool TraceReader::Seek(uint64_t trace_idx) {
	ASSERT_CLOSED(false);
	return f_trace_reader_->Seek(trace_idx);
}

bool TraceReader::HasNext() {
	ASSERT_CLOSED(false);
	return f_trace_reader_->HasNext();
}

bool TraceReader::Close() {
	if(is_closed_) {
		return true;
	} else {
		if(!f_trace_reader_->Close()) {
			sc_errno = SCCombineErrno(SC_ERR_CLOSE_FILE, errno);
			return false;
		}
		if(!ReaderBase::Close())
			return false;
		is_closed_ = true;
		return true;
	}
}

int TraceReader::GetTraceSize() {
	ASSERT_CLOSED(-1);
	return f_trace_reader_->GetTraceSize();
}//return length of one trace, in bytes

bool TraceReader::Get(uint64_t trace_idx, void* trace) {
	ASSERT_CLOSED(false);
	if(!f_trace_reader_->Get(trace_idx, trace)) {
		sc_errno = SCCombineErrno(SC_ERR_FRD, errno);
		return false;
	}
	return true;
}

bool TraceReader::Next(void* trace) {
	ASSERT_CLOSED(false);
	if(!f_trace_reader_->Next(trace)) {
		sc_errno = SCCombineErrno(SC_ERR_FRD, errno);
		return false;
	}
	return true;
}

TraceGather *TraceReader::NextGather() {
	ASSERT_CLOSED(NULL);
	if(NULL == order_) {
		return NULL;
	}
	TraceGather *trace_gather = new TraceGather(); //TODO
	return trace_gather;
}

bool TraceReader::SeekToGather(int gather_id) {
	ASSERT_CLOSED(false);
	if(NULL == order_) {
		return false;
	}

	return true;//TODO
}

TraceGather::TraceGather() {

}
TraceGather::~TraceGather() {

}

bool TraceGather::Next(void *trace) {

}
bool TraceGather::GetAll(void *trace) {

}

}
}



