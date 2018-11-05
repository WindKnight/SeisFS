/*
 * seisfile_prl_reader.cpp
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */
#include "seisfile_prl_reader.h"
#include "seisfile_headreader.h"
#include "seisfile_tracereader.h"

#include "util/seisfs_util_log.h"

namespace seisfs {
namespace file {

ParallelReader::ParallelReader(const std::string& data_name,StoreType real_store_type, int thrd_num, int task_grain) :
		SpeedReader(data_name, real_store_type, thrd_num, task_grain){

	active_ = NULL;
	sem_ = NULL;
}
ParallelReader::~ParallelReader() {
	if(NULL != active_) {
		delete active_;
		active_ = NULL;
	}
	if(NULL != sem_) {
		delete sem_;
		sem_ = NULL;
	}
}

bool ParallelReader::read(TaskType task_type, char *buf) {
	int start = 0, end;
	int left_trace_num = global_trace_id_.size();
	int task_num = 0;
	while(left_trace_num > 0) {
		int read_num = left_trace_num < task_grain_ ? left_trace_num : task_grain_;
		end = start + read_num - 1;
		PrlReadTask *task;
		if(task_type & READ_HEAD) {
			task = new PrlReadTaskHead(seis_file_, &head_filter_, &global_trace_id_, sem_);
			task->SetTask(buf + (int64_t)start * head_size_, start, end);
		} else if(task_type & READ_TRACE) {
			task = new PrlReadTaskTrace(seis_file_, &global_trace_id_, sem_);
			task->SetTask(buf + (int64_t)start * trace_size_, start, end);
		}
		start += read_num;
		left_trace_num -= read_num;
		active_->send(task);
		task_num ++;
	}
//	GPP_WAIT_UNTIL(active_->isEmpty());
	sem_->acquire(task_num);
	return true;
}

bool ParallelReader::Init() {
	if(!SpeedReader::Init()) {
		return false;
	}

	active_ = new GPP::Active<int>(thrd_num_, true);
	if(NULL == active_) {
		return false;
	}
	sem_ = new GPP::Semaphore();
	if(NULL == sem_) {
		return false;
	}
	return true;
}

bool ParallelReader::Close() {
	return true;
}

PrlReadTask::PrlReadTask(SharedPtr<SeisFile> seis_file, std::vector<int64_t> *p_global_trace_id, GPP::Semaphore *sem) {
	seis_file_ = seis_file;
	p_global_trace_id_ = p_global_trace_id;
	sem_ = sem;

}
PrlReadTask::~PrlReadTask() {

}

void PrlReadTask::SetTask(char *buf, int start, int end) {
	buf_ = buf;
	start_ = start;
	end_ = end;
}

int PrlReadTask::execute() {
	char *p_read = buf_;
	for(int trace_i = start_; trace_i <= end_; ++trace_i) {
		if(!ReadData((*p_global_trace_id_)[trace_i])) {
			printf("Read Data error\n");
			return -1;
		}
	}
	sem_->release(1);
	return 0;
}

PrlReadTaskHead::PrlReadTaskHead(SharedPtr<SeisFile> seis_file, HeadFilter *head_filter, std::vector<int64_t> *p_global_trace_id, GPP::Semaphore *sem) :
		PrlReadTask(seis_file, p_global_trace_id, sem){
	head_reader_ = seis_file_->OpenHeadReader();
	head_reader_->SetHeadFilter(*head_filter);
	head_size_ = head_reader_->GetHeadSize();

}
PrlReadTaskHead::~PrlReadTaskHead() {
	head_reader_->Close();
	if(NULL != head_reader_) {
		delete head_reader_;
		head_reader_ = NULL;
	}
}

bool PrlReadTaskHead::ReadData(int64_t trace_id) {
	bool ret = head_reader_->Get(trace_id, buf_);
	if(!ret) {
		return false;
	}
	buf_ += head_size_;
	return true;
}

PrlReadTaskTrace::PrlReadTaskTrace(SharedPtr<SeisFile> seis_file, std::vector<int64_t> *p_global_trace_id, GPP::Semaphore *sem) :
		PrlReadTask(seis_file, p_global_trace_id, sem) {
	trace_reader_ = seis_file_->OpenTraceReader();
	trace_size_ = seis_file_->GetTraceSize();
}
PrlReadTaskTrace::~PrlReadTaskTrace() {
	if(NULL != trace_reader_) {
		delete trace_reader_;
		trace_reader_ = NULL;
	}
}

bool PrlReadTaskTrace::ReadData(int64_t trace_id) {

	bool ret = trace_reader_->Get(trace_id, buf_);
	if(!ret) {
		return false;
	}
	float *p = (float*)buf_;
	printf("Trace read task, trace_id = %d, value = %f\n", trace_id, p[0]);fflush(stdout);
	buf_ += trace_size_;
	return true;
}


}
}



