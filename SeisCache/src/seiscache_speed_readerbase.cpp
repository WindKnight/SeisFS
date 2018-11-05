/*
 * seiscache_speed_readerbase.cpp
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */

#include "seiscache_speed_readerbase.h"
#include "seiscache_error.h"
#include "seisfile_prl_reader.h"
#include "seisfile_dis_reader.h"
#include "seisfile_headreader.h"
#include "util/seisfs_scoped_pointer.h"

namespace seisfs {
namespace cache {

SpeedReaderBase::SpeedReaderBase(const std::string data_name, StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
	data_name_(data_name), real_store_type_(real_store_type), meta_manager_(meta_manager){
	f_spd_reader_ = NULL;
}
SpeedReaderBase::~SpeedReaderBase() {
	if(NULL != f_spd_reader_) {
		delete f_spd_reader_;
		f_spd_reader_ = NULL;
	}
}

bool SpeedReaderBase::Init() {
	ScopedPointer<file::SeisFile> seis_file(file::SeisFile::New(data_name_, real_store_type_));
	ASSERT_NEW(seis_file.data(), false);
	if(!seis_file->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return false;
	}


	head_size_ = seis_file->GetHeadSize();
	trace_size_ = seis_file->GetTraceSize();
	trace_num_ = seis_file->GetTraceNum();

	std::string sf_classname = seis_file->GetClassName();

	int thrd_num = DISFETCH_THRD_NUM;
	int task_grain = DISFETCH_TASK_GRAIN;

	if(sf_classname == HDFS_CLASS_NAME) {
		int rebalance = DISFETCH_REBALANCE;
//		std::string hostFile = "./hosts";
		f_spd_reader_ = new file::DisReader(data_name_, real_store_type_, thrd_num, task_grain, rebalance);
	} else {
		f_spd_reader_ = new file::ParallelReader(data_name_, real_store_type_, thrd_num, task_grain);
	}
	ASSERT_NEW(f_spd_reader_, false);
	if(!f_spd_reader_->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FSPD_RD_INIT, errno);
	}

	is_closed_ = false;
	return true;
}

int64_t SpeedReaderBase::GetTraceNum() {
	ASSERT_CLOSED(-1);
	return trace_num_;
} //return trace num after filter.
int SpeedReaderBase::GetHeadSize() {
	ASSERT_CLOSED(-1);

	return head_size_;

}//return length of one head after filter, in bytes
int SpeedReaderBase::GetTraceSize() {
	ASSERT_CLOSED(-1);
	return trace_size_;

}//return length of one trace, in bytes

bool SpeedReaderBase::SetHeadFilter(SharedPtr<HeadFilter> head_filter) {
	ASSERT_CLOSED(false);
	head_filter_ = head_filter;

	if(NULL != head_filter_) {
		f_spd_reader_->SetHeadFilter(*head_filter_);
		head_size_ = f_spd_reader_->GetHeadSize();

	}

	return true;
}
bool SpeedReaderBase::SetRowFilter(SharedPtr<RowFilter> row_filter) {
	ASSERT_CLOSED(false);
	row_filter_ = row_filter;
	const std::vector<RowScope> scope_arr = row_filter_->GetAllScope();
	for(std::vector<RowScope>::const_iterator it = scope_arr.begin(); it != scope_arr.end(); ++it) {
		RowScope scope = *it;
		int64_t start = scope.GetStartTrace();
		int64_t end = scope.GetStopTrace();
		for(int64_t i = start; i <= end; ++i) {
			gather_.push_back(i);
		}
	}

	f_spd_reader_->SetGather(gather_);

	if(!gather_.empty())
		trace_num_ = gather_.size();

	return true;
}

bool SpeedReaderBase::SetRowFilter(SharedPtr<RowFilterByKey> row_filter_by_key) {
	ASSERT_CLOSED(false);

	return true;
}
bool SpeedReaderBase::SetOrder(SharedPtr<Order> order) {
	ASSERT_CLOSED(false);

	return true;
}

bool SpeedReaderBase::Close() {
	if(is_closed_) {
		return true;
	} else {
		if(!f_spd_reader_->Close()) {
			return false;
		}
		if(!meta_manager_->RecordLastAccessed())
			return false;

		return true;
	}

}

}
}


