/*
 * seisfile_prl_reader.h
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_PRL_READER_H_
#define SEISFILE_PRL_READER_H_


#include "seisfile_speed_reader.h"
#include <gpp.h>

namespace seisfs {
namespace file {

class ParallelReader : public SpeedReader {
public:
	ParallelReader(const std::string& data_name,StoreType real_store_type, int thrd_num, int task_grain);
	virtual ~ParallelReader();

	virtual bool Init();

	virtual bool Close();

private:
	virtual bool read(TaskType task_type, char *buf);

	GPP::Active<int> *active_;
	GPP::Semaphore *sem_;
};


class PrlReadTask : public GPP::Active<int>::Message {
public:
	PrlReadTask(SharedPtr<SeisFile> seis_file, std::vector<int64_t> *p_global_trace_id, GPP::Semaphore *sem);
	virtual ~PrlReadTask();

	void SetTask(char *buf, int start, int end);
	virtual int execute();
	virtual bool ReadData(int64_t trace_id) = 0;

protected:
	std::vector<int64_t> *p_global_trace_id_;
	char *buf_;
	int start_, end_;
	SharedPtr<SeisFile> seis_file_;
	GPP::Semaphore *sem_;
};


class PrlReadTaskHead : public PrlReadTask {
public:
	PrlReadTaskHead(SharedPtr<SeisFile> seis_file,  HeadFilter *head_filter, std::vector<int64_t> *p_global_trace_id, GPP::Semaphore *sem);
	virtual ~PrlReadTaskHead();

	virtual bool ReadData(int64_t trace_id);
private:
	int head_size_;
	HeadReader *head_reader_;

};


class PrlReadTaskTrace : public PrlReadTask {
public:
	PrlReadTaskTrace(SharedPtr<SeisFile> seis_file, std::vector<int64_t> *p_global_trace_id, GPP::Semaphore *sem);
	virtual ~PrlReadTaskTrace();

	virtual bool ReadData(int64_t trace_id);
private:
	int trace_size_;
	TraceReader *trace_reader_;

};



}
}

#endif /* SEISFILE_PRL_READER_H_ */
