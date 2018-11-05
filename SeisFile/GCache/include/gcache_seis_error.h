/*
 * gcache_seis_error.h
 *
 *  Created on: Jul 5, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_ERROR_H_
#define GCACHE_SEIS_ERROR_H_

#include <string>
#include <gcache_global.h>

NAMESPACE_BEGIN_SEISFS
namespace file {
#define GCACHE_SEIS_ERR_BASE        -20000
enum GCACHE_SEIS_ERRNO {
    GCACHE_SEIS_SUCCESS = 0,
    GCACHE_SEIS_ERR_PUTMETA = (GCACHE_SEIS_ERR_BASE + 1),
    GCACHE_SEIS_ERR_GETMETA = (GCACHE_SEIS_ERR_BASE + 2),
    GCACHE_SEIS_ERR_NOT_SUPPORT = (GCACHE_SEIS_ERR_BASE + 3),
    GCACHE_SEIS_ERR_OPFILE = (GCACHE_SEIS_ERR_BASE + 4),
    GCACHE_SEIS_ERR_MERGE_FAULT = (GCACHE_SEIS_ERR_BASE + 5),
    GCACHE_SEIS_ERR_SEEK = (GCACHE_SEIS_ERR_BASE + 6),
    GCACHE_SEIS_ERR_STAT = (GCACHE_SEIS_ERR_BASE + 7),
    GCACHE_SEIS_ERR_ROOTDIR = (GCACHE_SEIS_ERR_BASE + 8),
    GCACHE_SEIS_ERR_GETFS = (GCACHE_SEIS_ERR_BASE + 9),
    GCACHE_SEIS_ERR_RMDIR = (GCACHE_SEIS_ERR_BASE + 10),
    GCACHE_SEIS_ERR_RENAME = (GCACHE_SEIS_ERR_BASE + 11),
    GCACHE_SEIS_ERR_WRITE = (GCACHE_SEIS_ERR_BASE + 12),
    GCACHE_SEIS_ERR_READ = (GCACHE_SEIS_ERR_BASE + 13),
    GCACHE_SEIS_ERR_RMFILE = (GCACHE_SEIS_ERR_BASE + 14),
    GCACHE_SEIS_ERR_NOT_EXIST = (GCACHE_SEIS_ERR_BASE + 15),
    GCACHE_SEIS_ERR_TELL = (GCACHE_SEIS_ERR_BASE + 16),
    GCACHE_SEIS_ERR_DATAMISSING = (GCACHE_SEIS_ERR_BASE + 17),
    GCACHE_SEIS_ERR_FLUSH = (GCACHE_SEIS_ERR_BASE + 18),
    GCACHE_SEIS_ERR_CLOSE = (GCACHE_SEIS_ERR_BASE + 19),
    GCACHE_SEIS_ERR_DELETE = (GCACHE_SEIS_ERR_BASE + 20),
    GCACHE_SEIS_ERR_TRUNCATE = (GCACHE_SEIS_ERR_BASE + 21),
    GCACHE_SEIS_ERR_FILTER = (GCACHE_SEIS_ERR_BASE + 22),
    GCACHE_SEIS_ERR_PARAMERR = (GCACHE_SEIS_ERR_BASE + 23),
    GCACHE_SEIS_ERR_LISTDIRECTORY = (GCACHE_SEIS_ERR_BASE + 24),
    GCACHE_SEIS_ERR_SYNC = (GCACHE_SEIS_ERR_BASE + 25),
    GCACHE_SEIS_ERR_HFLUSH = (GCACHE_SEIS_ERR_BASE + 26),
    GCACHE_SEIS_ERR_CREATEDIR = (GCACHE_SEIS_ERR_BASE + 27),
    GCACHE_SEIS_ERR_GETFILEHANDLE = (GCACHE_SEIS_ERR_BASE + 28),
    GCACHE_SEIS_ERR_LOADINDEX = (GCACHE_SEIS_ERR_BASE + 29)
};

const char * const gcache_seis_errlist[] = { "no error",                                         //0
        "meta does not exist or meta size does not correct when put meta", //1
        "meta does not exist or meta size does not correct when get meta", //2
        "this kind of operation is not supported", //3
        "file or directory does not exist when open file",           //4
        "headType or traceType is not correct",                      //5
        "errno not used, please ignore",                             //6
        "file info does not exist",                                  //7
        "environment SEIS_ROOT_DIR is not set",                      //8
        "HDFS file system connect filed",                            //9
        "failed to delete seisData",                                //10
        "failed to rename seisData",                                //11
        "failed to write to HDFS or write uncomplete",              //12
        "failed to read data or read uncomplete",                   //13
        "failed to delete reel",                                    //14
        "file or directory does not exist ",                        //15
        "hdfsTell return false",                                    //16
        "data is corrupted",                                        //17
        "number out of range",                                      //18
        "hdfsFlush failed",                                         //19
        "hdfsCloseFile failed",                                     //20
        "hdfsDelete failed",                                        //21
        "hdfsTruncate failed",                                      //22
        "set filter failed, start_trace>=stop_trace",               //23
        "parameter error",                                          //24
        "hdfsDelete failed",                                        //25
        "Hdfs flush failed",                                      //26
        "Cannot creat dir",                                        //27
        "Get file handle failed",                                   //28
        "failed to load index of update log file",                  //29
        };

const int gcache_seis_nerr = sizeof(gcache_seis_errlist) / sizeof(gcache_seis_errlist[0]);

int GcacheSeisCombineErrno(int gcache_errno, int sys_errno);

std::string GcacheSeisStrerror(int err_no);

extern __thread int GCACHE_seis_errno;

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_ERROR_H_ */
