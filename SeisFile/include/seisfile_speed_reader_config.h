/*
 * seisfile_spd_reader_config.h
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_SPEED_READER_CONFIG_H_
#define SEISFILE_SPEED_READER_CONFIG_H_

#include <stdint.h>
#include "seisfs_config.h"

#define HDFS_HOST  "dn10-00"
#define HDFS_PORT  9000

#define HDFS_BUF_SIZE          1048576    //16384
#define HDFS_REPLICATION       3
#define HDFS_BLOCK_SIZE        67108864

#define INIT_BARRIER_TAG		1000
#define EXEC_BARRIER_TAG        1001
#define TASK_SCHEDULER_TAG		1002
#define TASK_EXECUTOR_TAG       1003
#define DATA_COLLECT_TAG        1004
#define TASK_REQUEST_TAG        1005

#define PARA_MQ_NORMAL          1
#define PARA_MQ_END             2
#define PARA_QUEUE_SIZE			512
#define DATA_QUEUE_SIZE        	512
#define PARA_SHM_SIZE           1048576     //1MB
#define DATA_SHM_SIZE           536870912   //512MB

#define TRACE_QUEUE_CAPACITY    50000

#define GBM_CONNECTION

#define HOST_NAME_BUF_LEN                   256

struct para_msg_st {
        long int msg_type;
        char paraBuf[PARA_QUEUE_SIZE];
};
struct data_msg_st {
        long int msg_type;
        char dataBuf[DATA_QUEUE_SIZE];
};

typedef struct TraceOffsetTag {
	int64_t globalOff; //trace id in seis data
	int localOff; //trace id in the gather of one Read call
} TraceOffset;

#define READ_HEAD	0x01
#define READ_TRACE	0x02

typedef int8_t TaskType;


#endif /* SEISFILE_SPEED_READER_CONFIG_H_ */
