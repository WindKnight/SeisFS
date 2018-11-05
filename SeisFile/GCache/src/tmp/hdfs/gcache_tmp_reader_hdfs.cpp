/*
 * gcache_tmp_reader_hdfs.cpp
 *
 *  Created on: Dec 29, 2015
 *      Author: wb
 */

#include <cerrno>

#include "gcache_tmp_reader_hdfs.h"
#include "gcache_tmp_error.h"
#include "gcache_hdfs_utils.h"
#include "gcache_log.h"
#include "gcache_io.h"

#define MAX_READ_SIZE 1073741824 //1G

NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * constructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param                fd is file descriptor
 *                                 fs is hdfs filesystem internal wrapper
 * @func                set member variable to initial values
 * @return                no return
 */
ReaderHDFS::ReaderHDFS(hdfsFile fd, hdfsFS fs, boost::shared_ptr<bool> isWriterAlive,
        boost::shared_ptr<std::string> sharedFileName) :
        _fd(fd), _fs(fs) {
    _isWriterAlive = isWriterAlive;
    _sharedFileName = sharedFileName;
}

/**
 * destructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                close hdfs file.
 * @return      no return.
 */
ReaderHDFS::~ReaderHDFS() {
    hdfsCloseFile(_fs, _fd);
}

/**
 * destructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                do some initialization.
 * @return                if an error occurs, return false.
 */
bool ReaderHDFS::Init() {
    return true;
}

/**
 * read
 *
 * @author          weibing
 * @version                0.1.4
 * @param                data is destination of read, max_len is read size
 * @func                Reads at most max_len bytes from the device into data.
 * @return                return the number of bytes read, if an error occurs, return -1.
 */
int64_t ReaderHDFS::Read(char *data, int64_t max_len) {
    int64_t ret_len = 0;
    ret_len = SafeRead(_fs, _fd, data, max_len);
    return ret_len;
}

/**
 * ReadLine
 *
 * @author          weibing
 * @version                0.1.4
 * @param                s point to the destination, size is the max size including '\0' at the tail
 * @func                Reads  in  at  most one less than size characters from stream and stores them into the buffer pointed to by s.
 Reading stops after an EOF or a newline.
 If a newline is read, it is stored into the buffer.
 A ’\0’ is stored after the last character in the buffer.
 * @return                return the number of bytes read, if an error occurs, return -1.
 */
int64_t ReaderHDFS::ReadLine(char* s, int size) {
    char c = 0;
    int iRet = 0, read_len = 0;
    while (read_len < size - 1) {
        iRet = Read(&c, 1);
        if (iRet < 0) {
            return -1;
        }

        if (iRet != 0 && c != '\n') {
            s[read_len++] = c;
        } else {
            break;
        }
    }
    s[read_len] = '\0';
    return read_len;
}

/**
 * destructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param                s is the destination to store the line read
 * @func                Reads in characters from stream and stores them into the string s.
 Reading stops after an EOF or a newline.
 If a newline is read, it is stored into the buffer.
 * @return                return the number of bytes read, if an error occurs, return -1.
 */
int64_t ReaderHDFS::ReadLine(std::string& s) {
    s.clear();
    char c = 0;
    int iRet = 0;
    while (1) {
        iRet = Read(&c, 1);
        if (iRet < 0) {
            return -1;
        }

        if (iRet > 0 && c != '\n') {
            s.push_back(c);
        } else {
            break;
        }
    }
    return s.size();
}

/**
 * destructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param                offset is the file position related to whence
 *              whence  whence argument specifies how the offset should be interpreted,
 * @func                The function is used to change the file position of the file with descriptor filedes,
 *              SEEK_CUR and SEEK_END are not supported.
 * @return                return true if success, if an error occurs, return false.
 */
bool ReaderHDFS::Seek(int64_t offset, int whence) {
    if (*_isWriterAlive) {
        Err("when the writer is alive, Seek is not supported in hdfs\n");
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_NOT_SUPPORT, errno);
        return false;
    }
    int ret_off = -1;
    switch (whence) {
        case SEEK_SET: {
            ret_off = hdfsSeek(_fs, _fd, offset);
        }
            break;
        case SEEK_CUR: {
            int64_t curOffset = hdfsTell(_fs, _fd);
            if (curOffset == -1) {
                Err("get the current offset in the file is failed\n");
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_HDFSTELL, errno);
                return false;
            }
            ret_off = hdfsSeek(_fs, _fd, curOffset + offset);
        }
            break;
        case SEEK_END: {
            hdfsFileInfo * fileInfo = hdfsGetPathInfo(_fs, (*_sharedFileName).c_str());
            if (fileInfo == NULL) {
                Err("can not get the the information of the fiel\n");
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                return -1;
            }
            int64_t fileSize = fileInfo->mSize;
            ret_off = hdfsSeek(_fs, _fd, fileSize + offset);
        }
            break;
        default: {
            Err("the whence is wrong, please input the right value\n");
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_NOT_SUPPORT, errno);
            return -1;
        }
            break;
    }

    if (ret_off == -1) {
        return false;
    }

    return true;
}

NAMESPACE_END_SEISFS
}
