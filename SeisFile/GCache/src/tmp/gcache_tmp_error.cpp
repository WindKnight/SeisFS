/*
 * gcache_tmp_error.cpp
 *
 *  Created on: Apr 6, 2016
 *      Author: wyd
 */

#include "gcache_tmp_error.h"

#include <stdint.h>
#include <string.h>
#include <cstdio>

#include "gcache_string.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

int GcacheTemCombineErrno(int gcache_errno, int sys_errno) {
    int combine_errno = gcache_errno;
    uint16_t sys_err = (uint16_t) sys_errno;
    combine_errno = (combine_errno << 16) | sys_err;
    return combine_errno;
}

std::string GcacheTemStrerror(int err_no) {
    std::string ret_str;

    if (err_no >= 0) {
        ret_str = strerror(err_no);

        char buf[16];
        sprintf(buf, "%d", err_no);

        ret_str = ret_str + ", errno = " + buf;

        return ret_str;
    } else if (err_no < GCACHE_TMP_ERR_BASE) {
        int sys_errno = err_no & 0xFFFF;
        int gcache_errno = err_no >> 16;

        std::string gcache_err_str;
        int real_gcache_errno = gcache_errno - GCACHE_TMP_ERR_BASE;
        if (real_gcache_errno >= 0 && real_gcache_errno < gcache_nerr) {
            gcache_err_str = gcache_tmp_errlist[real_gcache_errno];
        } else {
            gcache_err_str = "Unknown errno :" + ToString(gcache_errno);
        }

        std::string sys_err_str = sys_errno == 0 ? "" : strerror(sys_errno);
        ret_str = gcache_err_str + " : " + sys_err_str;

        char buf[16];
        sprintf(buf, "%d", sys_errno);
        ret_str = ret_str + ", errno = " + buf;
        return ret_str;
    } else {

        int real_gcache_errno = err_no - GCACHE_TMP_ERR_BASE;

        if (real_gcache_errno >= 0 && real_gcache_errno < gcache_nerr) {
            ret_str = gcache_tmp_errlist[real_gcache_errno];
            char buf[16];
            sprintf(buf, "%d", err_no);
            ret_str = ret_str + ", errno = " + buf;
            return ret_str;
        } else {
            ret_str = "Unknown errno :" + ToString(err_no);
            return ret_str;
        }
    }
}

__thread int GCACHE_tmp_errno = 0;

NAMESPACE_END_SEISFS
}
