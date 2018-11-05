/*
 * gbm_error.h
 *
 *  Created on: Oct 16, 2013
 *      Author: zch
 */

#ifndef GCACHE_TMP_ERROR_H_
#define GCACHE_TMP_ERROR_H_

#include <string>
#include <gcache_global.h>

NAMESPACE_BEGIN_SEISFS
namespace tmp {

#define GCACHE_TMP_ERR_BASE        -10000
enum GCACHE_TMP_ERRNO {
    GCACHE_TMP_SUCCESS = 0,
    GCACHE_TMP_ERR_NOARCH = (GCACHE_TMP_ERR_BASE + 1),
    GCACHE_TMP_ERR_NOT_SUPPORT = (GCACHE_TMP_ERR_BASE + 2),
    GCACHE_TMP_ERR_NOT_EXIST = (GCACHE_TMP_ERR_BASE + 3),
    GCACHE_TMP_ERR_OPFILE = (GCACHE_TMP_ERR_BASE + 4),
    GCACHE_TMP_ERR_OPDIR = (GCACHE_TMP_ERR_BASE + 5),
    GCACHE_TMP_ERR_STAT = (GCACHE_TMP_ERR_BASE + 6),
    GCACHE_TMP_ERR_MKDIR = (GCACHE_TMP_ERR_BASE + 7),
    GCACHE_TMP_ERR_RMFILE = (GCACHE_TMP_ERR_BASE + 8),
    GCACHE_TMP_ERR_RMDIR = (GCACHE_TMP_ERR_BASE + 9),
    GCACHE_TMP_ERR_RENAME = (GCACHE_TMP_ERR_BASE + 10),
    GCACHE_TMP_ERR_NOTEMPTY = (GCACHE_TMP_ERR_BASE + 11),
    GCACHE_TMP_ERR_STORAGE = (GCACHE_TMP_ERR_BASE + 12),
    GCACHE_TMP_ERR_REPDIR = (GCACHE_TMP_ERR_BASE + 13),
    GCACHE_TMP_ERR_ADVICE = (GCACHE_TMP_ERR_BASE + 14),
    GCACHE_TMP_ERR_MAP = (GCACHE_TMP_ERR_BASE + 15),
    GCACHE_TMP_ERR_LINK = (GCACHE_TMP_ERR_BASE + 16),
    GCACHE_TMP_ERR_PUTMETA = (GCACHE_TMP_ERR_BASE + 17),
    GCACHE_TMP_ERR_GETMETA = (GCACHE_TMP_ERR_BASE + 18),
    GCACHE_TMP_ERR_DELMETA = (GCACHE_TMP_ERR_BASE + 19),
    GCACHE_TMP_ERR_RENMETA = (GCACHE_TMP_ERR_BASE + 20),
    GCACHE_TMP_ERR_WRITE = (GCACHE_TMP_ERR_BASE + 21),
    GCACHE_TMP_ERR_READ = (GCACHE_TMP_ERR_BASE + 22),
    GCACHE_TMP_ERR_NOTDIR = (GCACHE_TMP_ERR_BASE + 23),
    GCACHE_TMP_ERR_HDFSTELL = (GCACHE_TMP_ERR_BASE + 24),
    GCACHE_TMP_ERR_COMPRESS = (GCACHE_TMP_ERR_BASE + 25),
    GCACHE_TMP_ERR_CLOSE = (GCACHE_TMP_ERR_BASE + 26),
    GCACHE_TMP_ERR_CONF = (GCACHE_TMP_ERR_BASE + 27),
    GCACHE_TMP_ERR_SPACE_FULL = (GCACHE_TMP_ERR_BASE + 28),
    GCACHE_TMP_ERR_GETFS = (GCACHE_TMP_ERR_BASE + 29),
    GCACHE_TMP_ERR_PATH = (GCACHE_TMP_ERR_BASE + 30),
    GCACHE_TMP_ERR_TRUNCATE = (GCACHE_TMP_ERR_BASE + 31),
    GCACHE_TMP_ERR_GETFSREAD = (GCACHE_TMP_ERR_BASE + 32),
    GCACHE_TMP_ERR_GETFSWRITE = (GCACHE_TMP_ERR_BASE + 33),
    GCACHE_TMP_ERR_LIFETIME = (GCACHE_TMP_ERR_BASE + 34),
    GCACHE_TMP_ERR_GETOBJNAME = (GCACHE_TMP_ERR_BASE + 35),
    GCACHE_TMP_ERR_CALEV = (GCACHE_TMP_ERR_BASE + 36),
    GCACHE_TMP_ERR_SEEK = (GCACHE_TMP_ERR_BASE + 37),
    GCACHE_TMP_ERR_ACCSESS = (GCACHE_TMP_ERR_BASE + 38),
};

const char * const gcache_tmp_errlist[] = { "Success",                                   //0
        "No such storage architecture",                        //1
        "Path or flag is not support",                                                        //2
        "File or directory does not exist",                //3
        "Can not open file",                                        //4
        "Can not open dir",                                                //5
        "Can not get file/directory status",        //6
        "Can not make dir",                                                //7
        "Can not remove file",                                        //8
        "Can not remove dir",                                        //9
        "Can not rename",                                                //10
        "Directory is not empty",                                //11
        "Can not get storage information",                //12
        "Repeat Dir ID",                                                //13
        "Set advice error",                                                //14
        "Map failed",                                                        //15
        "Link error",                                                        //16
        "Put meta error",                                                //17
        "Get meta error",                                                //18
        "Delete meta error",                                        //19
        "Rename meta error",                                        //20
        "Write to file failed",                                        //21
        "Read file failed",                                                //22
        "Path is not a dir",                                        //23
        "the hdfsTell failed",                               //24
        "Compress failed",                                     //25
        "Close error",                                          //26
        "Conf error",                                           //27
        "Space is full",                                        //28
        "GetFs error",                                          //29
        "File or dir path error",                               //30
        "Truncate error",                                       //31
        "Get FS Reader error",                                  //32
        "Get FS Writer error",                                  //33
        "Lifetime is null",                                     //34
        "Getobjname failed",                                    //35
        "Cachelevel error",                                     //36
        "Seek error",                                           //37
        "Access error"                                          //38
        };

const int gcache_nerr = sizeof(gcache_tmp_errlist) / sizeof(gcache_tmp_errlist[0]);

int GcacheTemCombineErrno(int gcache_errno, int sys_errno);

std::string GcacheTemStrerror(int err_no);

extern __thread int GCACHE_tmp_errno;

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_TMP_ERROR_H_ */
