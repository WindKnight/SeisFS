/*
 * seisfile_dis_reader.cpp
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */


#include "seisfile_dis_reader.h"
#include "util/seisfs_scoped_pointer.h"
#include "util/seisfs_util_log.h"
#include <fstream>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#ifdef USE_GCACHE
#include "gcache_seis_data.h"
#else
#include "seisfile_hdfs.h"
#endif

namespace seisfs {
namespace file {

DisReader::DisReader(const std::string& data_name, StoreType real_store_type, int thrd_num,
		int task_grain, int rebalance) :
		SpeedReader(data_name, real_store_type, thrd_num, task_grain){

	_rebalance = rebalance;
	host_path_ = mkHostFile();
//	host_path_ = "/s0/ext/extwzb3/wyd/workspace/SeisFS/SeisFS-0.4/SeisCache/test/hosts";
}

std::string DisReader::mkHostFile() {
	srand(time(NULL));
	int randNum = rand();
	char buf[4096]; sprintf(buf,"%d",randNum);
	std::string randStr = buf;
	std::string hostFile= std::string("/tmp/disfetch_srand.") + std::string(buf);
	std::string curHostName = std::string(getenv("HOST"));
	std::string hdSlavesFile = std::string(getenv("HADOOP_HOME")) + "/etc/hadoop/slaves";
	std::ifstream in(hdSlavesFile.c_str());
	std::ofstream out(hostFile.c_str());
	out<<curHostName<<std::endl;
	std::string hostName;
	while(std::getline(in,hostName)) {
		if(hostName.empty())
			continue;
		out<<hostName<<std::endl;
	}
	return hostFile;
}

DisReader::~DisReader() {
	if(!is_closed_) {
		Close();
	}
}

bool DisReader::Init() {

	if(!SpeedReader::Init()) {
		return false;
	}


	open_mutex_.lock();
	if(open_num_ <= 0) {


		int paraMQKey, dataMQKey, paraShmKey, dataShmKey;
		getMQKey(paraMQKey, dataMQKey, paraShmKey, dataShmKey);

		printf("create para message queue...\n");fflush(stdout);
		clearMQ(paraMQKey);
		_paraMQId = msgget((key_t)paraMQKey,0666|IPC_CREAT);
	    if(_paraMQId == -1) {
	    	fprintf(stdout,"DisReader Creating Parameter Message Queue Failure with errno is %d\n",errno);
	    	perror("create para message queue failure:");
	    	return false;
	    }

	    printf("create data message queue...\n");fflush(stdout);
	    clearMQ(dataMQKey);
		_dataMQId = msgget((key_t)dataMQKey,0666|IPC_CREAT);
	    if(_dataMQId == -1) {
	        fprintf(stdout,"DisReader Creating Parameter Message Queue Failure with errno is %d\n",errno);
	        perror("create data message queue failure:");
	        return false;
	    }

	    printf("create para shared memory...\n");fflush(stdout);
	    //clearShm(paraShmKey);
		_paraShmId = shmget(paraShmKey, PARA_SHM_SIZE, 0666|IPC_CREAT);
	    if(_paraShmId == -1) {
	    	fprintf(stdout,"DisReader Creating Parameter Shared Memory Failure with errno is %d\n",errno);
	    	perror("create para shared memory failure:");
	    	return false;
	    }

	    printf("create data shared memory ...\n");fflush(stdout);
		//clearShm(dataShmKey);
	    _dataShmId = shmget(dataShmKey,DATA_SHM_SIZE, 0666|IPC_CREAT);
	    if(_dataShmId == -1) {
	        fprintf(stdout,"DisReader Creating Data Shared Memory Failure with errno is %d\n",errno);
	        perror("create data shared memory failure:");
	        return false;
	    }

		_paraMem = shmat(_paraShmId,(const void*)0,0);
		if( _paraMem == (void*) -1) {
			fprintf(stdout,"DisReader shmat para shared memory object failure!\n");fflush(stdout);
			return false;
		}
		_dataMem = shmat(_dataShmId,(const void*)0,0);
		if( _dataMem == (void*) -1) {
			fprintf(stdout,"DisReader shmat data shared memory object failure!\n");fflush(stdout);
			return false;
		}

	    //start framework
	#if 0
		std::string hostFile = mkHostFile();
		fprintf(stdout,"Host file path is %s\n",hostFile.c_str());fflush(stdout);
	#else
		std::string hostFile = host_path_; //TODO host
	#endif
	    char* seisfs_path = getenv("SEISFS_HOME");
	    char binPath[4096];
	    sprintf(binPath,"%s/bin/disfetch",seisfs_path);
	    fprintf(stdout,"binPath is %s\n",binPath);fflush(stdout);
	    char cmd[4096];
	    sprintf(cmd,"gppexec -h %s -e %s -a \"%s %d %d %d %d %d %d %d \" -f -l ssh &",
	    		hostFile.c_str(), binPath, data_name_.c_str(),
	    		paraMQKey, dataMQKey, paraShmKey, dataShmKey,
	    		thrd_num_, task_grain_, _rebalance);
	    fprintf(stdout,"cmd is %s\n",cmd);fflush(stdout);
	    if(0 != system(cmd)) {
	    	fprintf(stdout,"execuing cmd failure!\n");
	    	return false;
	    }
	}

	open_num_ ++;
	open_mutex_.unlock();

    is_closed_ = false;
    return true;
}


bool DisReader::Close() {

	open_mutex_.lock();
	open_num_ --;
	if(open_num_ <= 0) {
		printf("sending quit info ...\n");fflush(stdout);
		_paraMsg.msg_type = 1;
		int traceNumber = -1;
		memcpy(_paraMsg.paraBuf,&traceNumber,sizeof(int));
		if(msgsnd(_paraMQId,(void*)&_paraMsg,PARA_QUEUE_SIZE,0)==-1)
		{
			fprintf(stdout,"Sending to Parameter Message Queue failure with errno %d\n",errno);fflush(stdout);
			perror("mq failure:");
			return false;
		}
		sleep(1);

		//close framework
		if(msgctl(_paraMQId,IPC_RMID,0) == -1) {
			fprintf(stdout,"DisReader msgctl(IPC_RMID) _paraMQId failure with errno is %d\n",errno);
			return false;
		}
		if(msgctl(_dataMQId,IPC_RMID,0) == -1) {
			fprintf(stdout,"DisReader msgctl(IPC_RMID) _dataMQId failure with errno is %d\n",errno);
			return false;
		}
		if(0 != shmdt(_paraMem)) {
			fprintf(stdout,"DisReader shmat para shared memory object failure!\n");fflush(stdout);
			return false;
		}
		if(0 != shmdt(_dataMem)) {
			fprintf(stdout,"DisReader shmat data shared memory object failure!\n");fflush(stdout);
			return false;
		}
		if(shmctl(_paraShmId,IPC_RMID,0) == -1) {
			fprintf(stdout,"DisReader shmctl(IPC_RMID) _paraShmId failure with errno is %d\n",errno);
			return false;
		}
		if(shmctl(_dataShmId,IPC_RMID,0) == -1) {
			fprintf(stdout,"DisReader shmctl(IPC_RMID) _dataShmId failure with errno is %d\n",errno);
			return false;
		}
	}

	open_mutex_.unlock();

	is_closed_ = true;
	return true;
}

// return trace num read from storage
bool DisReader::read(TaskType task_type, char* readBuf) {


//	printf("1: Check Input Parameter ...\n");fflush(stdout);

	int totalCounter = global_trace_id_.size();
	int left_trace_num = totalCounter;

	if(totalCounter == 0) {
		return false;
	} else if(totalCounter < 0) {
		fprintf(stdout,"the totalCounter of vector offs is invalid!\n");fflush(stdout);
		return false;
	}


	int head_filter_key_num = head_filter_.NumKeys();
	std::list<int> head_filter_key_list = head_filter_.GetKeyList();

	int one_trace_size = task_type & READ_HEAD ? head_size_ : trace_size_;

	int read_grain = DATA_SHM_SIZE / one_trace_size;
//	int read_grain = task_grain_; //TODO

	int trace_i = 0;
	char *read_p = readBuf;

	while(left_trace_num > 0) {
		int read_trace_num = left_trace_num < read_grain ? left_trace_num : read_grain;
		int64_t read_size = read_trace_num * one_trace_size;

//		printf("2: Copy Offsets ...\n");fflush(stdout);

		int64_t memOff = 0, traceOff = 0;
		for(int i=0;i<read_trace_num;i++) {
			char * dstPtr = (char*)_paraMem + memOff;
			traceOff = global_trace_id_[trace_i];
			memcpy(dstPtr, &(traceOff), sizeof(int64_t));
			memOff += sizeof(int64_t);
			trace_i ++;
		}

		for(std::list<int>::iterator it = head_filter_key_list.begin(); it != head_filter_key_list.end(); ++it ) {
			int key_id = *it;

			printf("filter key_id in server is : %d\n", key_id);
			char * dstPtr = (char*)_paraMem + memOff;
			memcpy(dstPtr, &key_id, sizeof(key_id));
			memOff += sizeof(key_id);
		}


		//printf("3: Signal Start ...\n");fflush(stdout);
		//start pre-fetch

		_paraMsg.msg_type = task_type;
		char *msg_buf = _paraMsg.paraBuf;
		memcpy(msg_buf, &read_trace_num, sizeof(int));
		msg_buf += sizeof(int);
		memcpy(msg_buf, &head_filter_key_num, sizeof(int));
		if(msgsnd(_paraMQId,(void*)&_paraMsg,PARA_QUEUE_SIZE,0)==-1)
		{
			fprintf(stdout,"Sending to Parameter Message Queue failure with errno %d\n",errno);
			perror("mq failure:");
			exit(1);
		}

//		printf("4: Wait Signal ...\n");fflush(stdout);
		//wait data
		if(msgrcv(_dataMQId,(void*)&_dataMsg,DATA_QUEUE_SIZE,0,0) == -1) {
			fprintf(stdout,"msgrcv failure with errno is %d\n",errno);
			perror("error info:");
			exit(1);
		}

//		printf("5: Copying Data ...\n");fflush(stdout);
		//copy data
		memcpy(read_p,_dataMem,read_size);

		read_p += read_size;
		left_trace_num -= read_trace_num;
	}

	return true;
}


void DisReader::getMQKey(int& paraMQKey, int& dataMQKey, int& paraShmKey, int& dataShmKey) {
	paraMQKey = 1000;
	dataMQKey = 1001;
	paraShmKey = 1002;
	dataShmKey = 1003;
}

void DisReader::clearMQ(int mqKey) {
	int mqId = msgget((key_t)mqKey,0666|IPC_CREAT);
	if(mqId == -1) {
		fprintf(stdout,"Create message queue failure in clearMQ!\n");fflush(stdout);
		exit(1);
	}
	if(msgctl(mqId,IPC_RMID,0) == -1) {
		fprintf(stdout,"Destroy message queue failure in clearMQ!\n");
		exit(1);
	}
}

void DisReader::clearShm(int shmKey) {
    int shmId = shmget(shmKey,DATA_SHM_SIZE, 0666|IPC_CREAT);
    if(shmId == -1) {
        fprintf(stdout,"Create shared memory failure in clearShm!\n");fflush(stdout);
        exit(1);
    }
    if(msgctl(shmId,IPC_RMID,0) == -1) {
		fprintf(stdout,"Destroy shared memory failure in clearShm!\n");
		perror("Destroy Shared Memory");
		exit(1);
    }
}


int DisReader::open_num_ = 0;
GPP::Mutex DisReader::open_mutex_;

int DisReader::_paraMQId = 0;
int DisReader::_dataMQId = 0;
int DisReader::_paraShmId = 0;
int DisReader::_dataShmId = 0;

struct para_msg_st DisReader::_paraMsg;
struct data_msg_st DisReader::_dataMsg;

void * DisReader::_paraMem = NULL;
void * DisReader::_dataMem = NULL;

}
}

