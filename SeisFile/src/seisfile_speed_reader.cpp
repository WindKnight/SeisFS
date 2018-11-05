/*
 * seisfile_speed_readerbase.cpp
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */

#include "seisfile_speed_reader.h"
#include "seisfile_headreader.h"
#include "util/seisfs_util_log.h"
#include "util/seisfs_scoped_pointer.h"


namespace seisfs {
namespace file {

SpeedReader::SpeedReader(const std::string& data_name, StoreType real_store_type, int thrd_num, int task_grain) :
	data_name_(data_name), real_store_type_(real_store_type), thrd_num_(thrd_num), task_grain_(task_grain){

}
SpeedReader::~SpeedReader() {

}

bool SpeedReader::Init() {
	seis_file_ = seisfs::file::SeisFile::New(data_name_, real_store_type_);
	if(seis_file_ == NULL) {
		return false;
	}
	if(!seis_file_->Init()) {
		return false;
	}

	head_size_ = seis_file_->GetHeadSize();
	trace_size_ = seis_file_->GetTraceSize();

	return true;
}

void SpeedReader::SetGather(const std::vector<int64_t> &global_trace_id) {
	global_trace_id_ = global_trace_id;
}

void SpeedReader::SetHeadFilter(const HeadFilter &head_filter) {
	head_filter_ = head_filter;
	ScopedPointer<seisfs::file::HeadReader> head_reader (seis_file_->OpenHeadReader());
	head_reader->SetHeadFilter(head_filter_);
	head_size_ = head_reader->GetHeadSize();

	head_reader->Close();
}

int SpeedReader::GetTraceSize() {
	return trace_size_;
}
int SpeedReader::GetHeadSize() {

	return head_size_;
}

bool SpeedReader::ReadTrace(char *trace) {
	return read(READ_TRACE, trace);
}
bool SpeedReader::ReadHead(char *head) {
	return read(READ_HEAD, head);
}
bool SpeedReader::Read(char *head, char *trace) {

	bool ret = ReadTrace(trace);
	if(!ret) {
		return false;
	}

	ret = ReadHead(head);
	if(!ret) {
		return false;
	}

	return true;
}

bool SpeedReader::Close() {
	return true;
}

}
}


