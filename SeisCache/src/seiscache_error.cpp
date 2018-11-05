/*
 * seiscache_error.cpp
 *
 *  Created on: Apr 14, 2018
 *      Author: wyd
 */

#include "seiscache_error.h"
#include "util/seisfs_util_string.h"
#include <string.h>

namespace seisfs {

namespace cache {

int SCCombineErrno(int sc_errno, int sys_errno) {
	int combine_errno = sc_errno;
	uint16_t sys_err = (uint16_t) sys_errno;
	combine_errno = (combine_errno << 16) | sys_err;
	return combine_errno;
}


std::string SCStrerror(int err_no) {
	//system errno
	if(err_no > 0) {

		std::string errStr;

		char errBuf[256];
#if( (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && ! _GNU_SOURCE )
		int rc = strerror_r(err_no, errBuf, 256);
		if(rc < 0) {
			strcpy(errbuf, "undefined system error");
		}
		errStr = errBuf;
#else
		errStr = strerror_r(err_no, errBuf, 256);
#endif
		char buf[16];
		sprintf(buf, "%d", err_no);

		errStr = errStr + ", errno = " + buf;

		return errStr;
	}

	//system errno and sc_errno errno
	if(err_no < SC_ERR_BASE) {
		int sc_errno = err_no >> 16;
		unsigned int sysErrno = err_no & 0xFFFF;

		std::string errStr;

		int seis_errcode = sc_errno - SC_ERR_BASE;
		if (seis_errcode >= 0 && seis_errcode < sc_nerr) {
			errStr = sc_errlist[seis_errcode];
		} else {
			errStr = "undefined error " + Int2Str(seis_errcode);
		}

		char errBuf[256];
#if( (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && ! _GNU_SOURCE )
		int rc = strerror_r(sysErrno, errBuf, 256);
		if(rc < 0) {
			strcpy(errbuf, "undefined system error");
		}
		errStr = errStr + " : " + errBuf;
#else
		errStr = errStr + " : " + strerror_r(sysErrno, errBuf, 256);
#endif

		char buf[16];
		sprintf(buf, "%d", sysErrno);

		errStr = errStr + ", errno = " + buf;

		return errStr;
	}

  //sc_errno error
	int seis_errcode = err_no - SC_ERR_BASE;
	if (seis_errcode >= 0 && seis_errcode < sc_nerr) {

		std::string errStr = sc_errlist[seis_errcode];

		char buf[64];
		sprintf(buf, "%d", err_no);

		errStr = errStr + ", errno = " + buf;

		return errStr;
	}

	//undefined error
	std::string errStr = "undefined error : ";
	char buf[32];
	sprintf(buf, "%d", err_no);
	errStr = errStr + buf;
	return errStr;
}


__thread int sc_errno;

}
}
