/*
 * seisfile_dis_reader.h
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_DIS_READER_H_
#define SEISFILE_DIS_READER_H_

#include "seisfile_speed_reader.h"
#include <gpp.h>

namespace seisfs {
namespace file {

class DisReader : public SpeedReader {
public:
	DisReader(const std::string& data_name,StoreType real_store_type, int thrd_num,
			int task_grain, int rebalance);
	virtual ~DisReader();

	virtual bool Init();
	virtual bool Close();

private:
	bool read(TaskType task_type, char* buf);
	std::string mkHostFile();
	void getMQKey(int& paraMQKey, int& dataMQKey, int& paraShmKey, int& dataShmKey);
	void clearMQ(int mqKey);
	void clearShm(int shmKey);


	int _rebalance;
	std::string host_path_;

	bool is_closed_;

private:

	static int _paraMQId;
	static int _dataMQId;
	static int _paraShmId;
	static int _dataShmId;

	static struct para_msg_st _paraMsg;
	static struct data_msg_st _dataMsg;

	static void * _paraMem;
	static void * _dataMem;

	static int open_num_;
	static GPP::Mutex open_mutex_;;
};

}
}



#endif /* SEISFILE_DIS_READER_H_ */
