/*
 * gcache_tmp_writer_nas.cpp
 *
 *  Created on: Jan 7, 2016
 *      Author: wb
 */

#include <unistd.h>

#include "gcache_tmp_writer_nas.h"
#include "gcache_io.h"

#define LINUX_VERSION(x,y,z)        (0x10000*(x) + 0x100*(y) + z)
#define WRITE_BUF_LEN                        65536        //64KB

NAMESPACE_BEGIN_SEISFS
namespace tmp {

/**
 * constructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param                fd is file descriptor
 *                                 direct_write indicates use direct I/O or not
 * @func                set member variable to initial values.
 * @return                no return.
 */
WriterNAS::WriterNAS(int fd, boost::shared_ptr<bool> isWriterAlive) :
        _fd(fd) {
    _isWriterAlive = isWriterAlive;
    *_isWriterAlive = true;
}

/**
 * destructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                close fd,
 * @return      no return.
 */
WriterNAS::~WriterNAS() {
    close(_fd);
    *_isWriterAlive = false;
}

/**
 * Write
 *
 * @author          weibing
 * @version                0.1.4
 * @param       data is the data to be written
 *              max_len is the length of data to be written
 * @func                writes data to the file.
 * @return      if write successfully return the length of data written to file, else return -1.
 */
int64_t WriterNAS::Write(const char *data, int64_t max_len) {
    int64_t ret_len = 0;
    ret_len = SafeWrite(_fd, data, max_len);

    return ret_len;
}

/**
 * Seek
 *
 * @author          weibing
 * @version                0.1.4
 * @param       offset is the position of the file
 *              whence argument specifies how the offset should be interpreted
 * @func                change the file position of the file with descriptor fields.
 * @return      if seek successfully return true, else return false.
 */
bool WriterNAS::Seek(int64_t offset, int whence) {
    off_t ret_off = lseek(_fd, offset, whence);
    if (ret_off == (off_t) -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_SEEK, errno);
        return false;
    }
    return true;
}

/**
 * Position
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func            get the current file position.
 * @return      return the current file position.
 */
int64_t WriterNAS::Pos() {
    off_t offset = lseek(_fd, 0, SEEK_CUR);
    return (int64_t) offset;
}

/**
 * Truncate
 *
 * @author          weibing
 * @version                0.1.4
 * @param       length is the final size of the file
 * @func                change the size of filename to length,
 *              if length is shorter than the previous length, data at the end will be lost.
 * @return      if truncate successfully return true, else return false.
 */
bool WriterNAS::Truncate(int64_t length) {
    int iRet = ftruncate(_fd, length);
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_TRUNCATE, errno);
        return false;
    }
    return true;
}

/**
 * Sync
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                transfers all modified in-core data of the file to the disk device where that file resides.
 * @return      no return.
 */
void WriterNAS::Sync() {
    fsync(_fd);
}

NAMESPACE_END_SEISFS
}
