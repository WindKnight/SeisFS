/*
 * seisfile_speed_reader.h
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_SPEED_READER_H_
#define SEISFILE_SPEED_READER_H_

#include "seisfile_speed_reader_config.h"
#include "seisfs_config.h"
#include "seisfile.h"
#include "seisfs_head_filter.h"
#include "util/seisfs_shared_pointer.h"
#include <string>
#include <vector>

namespace seisfs {

namespace file {

class SpeedReader {
public:
	SpeedReader(const std::string& data_name, StoreType real_store_type, int thrd_num, int task_grain);
	virtual ~SpeedReader();

	virtual bool Init();

	void SetGather(const std::vector<int64_t> &global_trace_id);
	void SetHeadFilter(const HeadFilter &head_filter);

	int GetTraceSize();
	int GetHeadSize();

	bool ReadTrace(char *trace);
	bool ReadHead(char *head);
	bool Read(char *head, char *trace);
	virtual bool Close();

protected:
	std::string data_name_;
	SharedPtr<SeisFile> seis_file_;
	HeadFilter head_filter_;
	StoreType real_store_type_;
	int head_size_, trace_size_;
	int thrd_num_;
	int task_grain_;
	std::vector<int64_t> global_trace_id_;

private:
	virtual bool read(TaskType task_type, char *buf) = 0;
};


}

}

#endif /* SEISFILE_SPEED_READER_H_ */
