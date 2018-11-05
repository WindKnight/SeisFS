/*
 * gcache_tmp_writer_local.cpp
 *
 *  Created on: Jan 4, 2016
 *      Author: wyd
 */

#include <stdio.h>
#include <unistd.h>

#include "gcache_tmp_writer_local.h"
#include "gcache_io.h"

#define LINUX_VERSION(x,y,z)        (0x10000*(x) + 0x100*(y) + z)
#define WRITE_BUF_LEN                        65536        //64KB

NAMESPACE_BEGIN_SEISFS
namespace tmp {

WriterLocal::WriterLocal(int fd, boost::shared_ptr<bool> isWriterAlive) :
        _fd(fd) {
    _isWriterAlive = isWriterAlive;
    *_isWriterAlive = true;
}

WriterLocal::~WriterLocal() {
    close(_fd);
    *_isWriterAlive = false;
}

#if 0
bool WriterLocal::Init() {
    if(_direct_write) {
        static struct utsname uts;
        int x = 0, y = 0, z = 0; /* cleared in case sscanf() < 3 */

        if (uname(&uts) == -1) /* failure implies impending death */
        return false;
        if (sscanf(uts.release, "%d.%d.%d", &x, &y, &z) < 3) {
            return false;
        }

        if(LINUX_VERSION(x, y, z) > LINUX_VERSION(2, 6, 0)) {
            _block_size = 512;
        } else {
            struct statfs statfs_buf;
            if(0 == fstatfs(_fd, &statfs_buf)) {
                _block_size = statfs_buf.f_bsize;
            } else {
                return false;
            }
        }

        _direct_buf_len = WRITE_BUF_LEN;
        if(0 == posix_memalign((void **)&_direct_write_buf, _block_size, _direct_buf_len))
        return true;
        else
        return false;
    } else
    return true;

}
#endif

int64_t WriterLocal::Write(const char *data, int64_t max_len) {
    int64_t ret_len = 0;
#if 0
    if(!_direct_write) {
        ret_len = write(_fd, data, max_len);
    } else {
        int64_t left_write_len = max_len;
        int64_t copy_src_offset = 0;
        while(left_write_len > 0) {
            int copy_len = _direct_buf_len - _left_data_size_in_buf;
            copy_len = left_write_len > _direct_buf_len ? _direct_buf_len : left_write_len;

            memcpy(_direct_write_buf + _left_data_size_in_buf, data + copy_src_offset, copy_len);

            _left_data_size_in_buf += copy_len;
            copy_src_offset += copy_len;
            left_write_len -= copy_len;
            ret_len += copy_len;

            if(_left_data_size_in_buf >= _direct_buf_len) {
                int iRet = write(_fd, _direct_write_buf, _direct_buf_len);
                if (iRet != _direct_buf_len)
                return false;

                _left_data_size_in_buf = 0;
            }
        }
    }
#endif
    ret_len = SafeWrite(_fd, data, max_len);

    return ret_len;
}

bool WriterLocal::Seek(int64_t offset, int whence) {
    off_t ret_off = lseek(_fd, offset, whence);
    if (ret_off == (off_t) -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_SEEK, errno);
        return false;
    }
    return true;
}

int64_t WriterLocal::Pos() {
    off_t offset = lseek(_fd, 0, SEEK_CUR);
    return (int64_t) offset;
}

bool WriterLocal::Truncate(int64_t length) {

    int iRet = ftruncate(_fd, length);
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_TRUNCATE, errno);
        return false;
    }
    return true;
}

void WriterLocal::Sync() {
    fsync(_fd);
}

#if 0
bool WriterLocal::Flush() {
    int hole_size = 0;
    if(_left_data_size_in_buf > 0) {
        int iRet = write(_fd, _direct_write_buf, _direct_buf_len);
        if (iRet != _direct_buf_len)
        return false;

        hole_size = _direct_buf_len - _left_data_size_in_buf;
        _left_data_size_in_buf = 0;
    }

    off_t cur_off = lseek(_fd, 0, SEEK_CUR);

    int64_t real_file_size = cur_off - hole_size;
    int iRet = ftruncate(_fd, real_file_size);
    if(-1 == iRet)
    return false;
    return true;
}
#endif

NAMESPACE_END_SEISFS
}
