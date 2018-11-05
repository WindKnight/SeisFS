/*
 * seisfile_error.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_HDFS_ERROR_H_
#define SEISFILE_HDFS_ERROR_H_

#include <string>

namespace seisfs {

namespace file {

#define SEISFILE_ERR_BASE        -20000
enum SEISFILE_ERRNO {
    SEISFILE_SUCCESS = 0,
    SEISFILE_ERR_OPENMETA = 1,
    SEISFILE_ERR_NODATA = 2,
    SEISFILE_ERR_NOMETA = 3,
    SEISFILE_ERR_OPENFILE = 4,
    SEISFILE_ERR_TRACEINDEX = 5,
};

const char * const seisfile_errlist[] = { "no error",                                         //0
		"fail to create or open meta file",
		"no such data",
		"meta file does not exit",
		"fail to open file",
		"trace index out of range",
		"",
		"",
        };

const int seisfile_nerr = sizeof(seisfile_errlist) / sizeof(seisfile_errlist[0]);

int SeisFileCombineErrno(int gcache_errno, int sys_errno);

std::string SeisFileStrerror(int err_no);

extern __thread int GCACHE_seis_errno;

}
}

#endif /* SEISFILE_HDFS_ERROR_H_ */
