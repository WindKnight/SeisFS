/*
 * seiscache_error.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_ERROR_H_
#define SEISCACHE_ERROR_H_

#include <string>
#include <errno.h>

namespace seisfs {

namespace cache {


#define ASSERT_NEW(pointer, ret)	\
	if(NULL == pointer) {	\
		sc_errno = SCCombineErrno(SC_ERR_NEW_OBJ, errno);	\
		return ret;	\
	}

#define ASSERT(func, ret)	\
		if(!func) {	\
			return ret;	\
		}

#define ASSERT_CLOSED(ret)	\
		if(is_closed_) {	\
			sc_errno = SC_ERR_CLOSED;	\
			return ret;	\
		}

#define SC_SUCCESS				0

#define SC_ERR_BASE				-10000

#define SC_ERR_NOVAL					SC_ERR_BASE + 1
#define SC_ERR_NEW_OBJ					SC_ERR_BASE + 2
#define SC_ERR_FILE_INIT				SC_ERR_BASE + 3
#define SC_ERR_FSPD_RD_INIT				SC_ERR_BASE + 4
#define SC_ERR_INTEGRITY				SC_ERR_BASE + 5
#define SC_ERR_CLOSED					SC_ERR_BASE + 6
#define SC_ERR_OP_FRD					SC_ERR_BASE + 7
#define SC_ERR_OP_FWR					SC_ERR_BASE + 8
#define SC_ERR_OP_FUP					SC_ERR_BASE + 9
#define SC_ERR_PUT_META					SC_ERR_BASE + 10
#define SC_ERR_GET_META					SC_ERR_BASE + 11
#define SC_ERR_OP_INVAL_WR				SC_ERR_BASE + 12
#define SC_ERR_RM_FILE					SC_ERR_BASE + 13
#define SC_ERR_RM_META					SC_ERR_BASE + 14
#define SC_ERR_BUSY						SC_ERR_BASE + 15
#define SC_ERR_RNAM_FILE				SC_ERR_BASE + 16
#define SC_ERR_CLOSE_FILE				SC_ERR_BASE + 17
#define SC_ERR_MG_DIF_TYPE				SC_ERR_BASE + 18
#define SC_ERR_MERGE_FILE				SC_ERR_BASE + 19
#define SC_ERR_MERGE_FKV				SC_ERR_BASE + 20
#define SC_ERR_REG_QUEUE				SC_ERR_BASE + 21
#define SC_ERR_COMPACT					SC_ERR_BASE + 22
#define SC_ERR_FRD						SC_ERR_BASE + 23
#define SC_ERR_CPY_META					SC_ERR_BASE + 24


//#define SC_ERR_OPEN_NEW				SC_ERR_BASE + 2
//#define SC_ERR_OPEN_EXIST				SC_ERR_BASE + 3
//#define SC_ERR_INVAL_STYPE			SC_ERR_BASE + 4
//#define SC_ERR_OPEN_INVAL				SC_ERR_BASE + 5

//#define SC_ERR_REG_QUEUE				SC_ERR_BASE + 7


const char *const sc_errlist[] = {
		"Undefined error",												//0
		"There is no valid data",										//1
		"New object error",												//2
		"Init seis file error",											//3
		"Init seis file speed reader error",							//4
		"Data one different storage media have different trace num",	//5
		"This handler has been closed",									//6
		"Open seis file reader error",									//7
		"Open seis file writer error",									//8
		"Open seis file updater error",									//9
		"Put meta error",												//10
		"Get meta error",												//11
		"There has been data on other storage media, can not "
		"open new data for writing on the specified storage type",		//12
		"Remove seis file error",										//13
		"Remove seis meta error",										//14
		"This data is busy",											//15
		"Rename seis file error",										//16
		"Close file reader/writer/updater error",						//17
		"Data in merge should be on the same storage medium",			//18
		"Merge seis file error",										//19
		"Merge seis file kvinfo error",									//20
		"Regist to write back queue error",								//21
		"Seis file on hdfs Compact error",								//22
		"Seis file read data error",										//23
		"Copy meta error"										//24



};

const int sc_nerr = sizeof(sc_errlist) / sizeof(sc_errlist[0]);


int SCCombineErrno(int seiscache_errno, int sys_errno);

std::string SCStrerror(int err_no);

extern __thread int sc_errno;

}

}


#endif /* SEISCACHE_ERROR_H_ */
