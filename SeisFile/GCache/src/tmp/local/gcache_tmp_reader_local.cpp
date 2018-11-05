/**
 * gcache_tmp_reader_local.cpp
 *
 *  Created on: Dec 29, 2015
 *      Author: wyd
 */

#include <stdio.h>
#include <string.h>
#include <cerrno>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/utsname.h>
#include <unistd.h>
#include <cstdlib>

#include "gcache_tmp_reader_local.h"
#include "gcache_io.h"
#include "gcache_tmp_error.h"
#include "gcache_log.h"

#define LINUX_VERSION(x,y,z)        (0x10000*(x) + 0x100*(y) + z)
#define READ_BUF_LEN                                 65536                //64k

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * constructor
 *
 * @author          wangyida
 * @version                0.1.2
 * @param                fd is file descriptor
 *                                 direct_read indicate use direct I/O or not
 *                                 map_buf is the start position of data mapped into memory, if we do not use mmap, it should be NULL.
 * @func                set member variable to initial values
 * @return                no return
 */
ReaderLocal::ReaderLocal(int fd, bool direct_read, char *map_buf) :
        _fd(fd), _map_buf(map_buf), _direct_read(direct_read) {
    /*
     * mmap read param
     */
    _read_ptr = _map_buf;
    _file_size = 0;

    /*
     * direct read param
     */
    _direct_read_buf = NULL;
    _copy_src_ptr = NULL;
    _block_size = 0;
    _direct_buf_len = 0;
    _left_data_size_in_buf = 0;
}

/**
 * destructor
 *
 * @author          wangyida
 * @version                0.1.2
 * @param
 * @func                close fd
 *                                 if we use mmap, call munmap
 *                                 if we use direct I/O, free the buffer
 * @return
 */

ReaderLocal::~ReaderLocal() {
    if (_map_buf != NULL) {
        munmap(_map_buf, _file_size);
        close(_fd);
    } else {
        close(_fd);
    }

    if (_direct_read) {
        free(_direct_read_buf);
    }

}

/**
 * destructor
 *
 * @author          wangyida
 * @version                0.1.2
 * @param
 * @func                do some complex initialization
 *                                 if we use mmap, get file size
 *                                 if we use direct I/O, get block size according to kernel version, and malloc aligned memory
 * @return                if an error occurs, returna false
 */

bool ReaderLocal::Init() {
    if (_map_buf != NULL) {
        struct stat stat_buf;
        int iRet = fstat(_fd, &stat_buf);
        if (0 == iRet) {
            _file_size = stat_buf.st_size;
            return true;
        } else {
            return false;
        }
    }
    if (_direct_read) {

        //(static)
        static struct utsname uts;
        int x = 0, y = 0, z = 0; /* cleared in case sscanf() < 3 */

        if (uname(&uts) == -1) /* failure implies impending death */
        return false;
        if (sscanf(uts.release, "%d.%d.%d", &x, &y, &z) < 3) {
            return false;
        }

        struct statfs statfs_buf;
        if (0 == fstatfs(_fd, &statfs_buf)) {
            _block_size = statfs_buf.f_bsize;
        } else {
//                        if(LINUX_VERSION(x, y, z) > LINUX_VERSION(2, 6, 0))
//                        {
//                                _block_size = 512;
//                        }
            return false;
        }

        _direct_buf_len = READ_BUF_LEN;
        if (0 == posix_memalign((void **) &_direct_read_buf, _block_size, _direct_buf_len)) return true;
        else return false;
    }
    return true;
}

/**
 * read
 *
 * @author          wangyida
 * @version                0.1.2
 * @param                data is destination of read, max_len is read size
 * @func                Reads at most max_len bytes from the device into data
 *                                 if we use normal read, just call read
 *                                 if we use direct I/O, we first have to read data to the _direct_read_buf,
 *                                 then copy data to the destination from accurate position
 *                                 if we use mmap, we have to copy data from map buffer to the destination, and update the read_ptr
 * @return                return the number of bytes read, if an error occurs, return -1
 */

int64_t ReaderLocal::Read(char *data, int64_t max_len) {
    int64_t ret_len;
    if (_map_buf == NULL) {
        if (!_direct_read) {
            ret_len = SafeRead(_fd, data, max_len);
        } else { // direct_read

            int64_t left_read_len = max_len;
            char *copy_dest_ptr = data;
            ret_len = 0;

            /*
             * if there is data that hasn't been read left in the buf, copy them to the dest first
             * in this case, there will be no hole
             */
            if (_left_data_size_in_buf > 0) {
                int64_t copy_len =
                        _left_data_size_in_buf > max_len ? max_len : _left_data_size_in_buf;
                memcpy(copy_dest_ptr, _copy_src_ptr, copy_len);
                _copy_src_ptr += copy_len;
                copy_dest_ptr += copy_len;
                _left_data_size_in_buf -= copy_len;
                left_read_len -= copy_len;
                ret_len += copy_len;

                if (left_read_len <= 0) return ret_len;
            }

            /*
             * if current offset is not block aligned, seek backward to the aligned offset.
             * then we get a hole, in witch the data is useless. record the hole len.
             */
            off_t cur_off = lseek(_fd, 0, SEEK_CUR);
            int64_t hole_len = cur_off % _block_size;
            if (hole_len > 0) {
                lseek(_fd, -hole_len, SEEK_CUR);
            }

            while (left_read_len > 0) {
                /*
                 * in each round, the data len we need to read is left_read_len + hole_len
                 */
                int64_t needed_read_len = left_read_len + hole_len;
                needed_read_len =
                        needed_read_len > _direct_buf_len ? _direct_buf_len : needed_read_len;
                int64_t read_block_num = (needed_read_len - 1) / _block_size + 1;

                int64_t read_ret = read(_fd, _direct_read_buf, read_block_num * _block_size);
                if (read_ret == -1) return -1;
                else if (read_ret == 0) break;
                else {

                    _copy_src_ptr = _direct_read_buf + hole_len;
                    _left_data_size_in_buf = read_ret - hole_len;

                    int64_t copy_len = needed_read_len < read_ret ? needed_read_len : read_ret;
                    copy_len -= hole_len;
                    copy_len = copy_len > 0 ? copy_len : 0;

                    memcpy(copy_dest_ptr, _copy_src_ptr, copy_len);

                    _copy_src_ptr += copy_len;
                    copy_dest_ptr += copy_len;
                    _left_data_size_in_buf -= copy_len;
                    left_read_len -= copy_len;
                    ret_len += copy_len;

                    /*
                     * hole_len is only useful in the first round
                     */
                    hole_len = 0;
                }
            }
        } // direct_read

    } else {
        int64_t left_size = _file_size - (_read_ptr - _map_buf);
        int64_t read_len = max_len > left_size ? left_size : max_len;
        if (read_len <= 0) {
            return 0;
        }
        memcpy(data, _read_ptr, read_len);
        _read_ptr += read_len;
        ret_len = read_len;
    }

    return ret_len;
}

/**
 * ReadLine
 *
 * @author          wangyida
 * @version                0.1.2
 * @param                s point to the destination, size is the max size including '\0' at the tail
 * @func                Reads  in  at  most one less than size characters from stream and stores them into the buffer pointed to by s.
 Reading stops after an EOF or a newline.
 If a newline is read, it is stored into the buffer.
 A ’\0’ is stored after the last character in the buffer.
 * @return                return the number of bytes read, if an error occurs, return -1
 */

int64_t ReaderLocal::ReadLine(char* s, int size) {
    char c = 0;
    int iRet = 0, read_len = 0;
    while (read_len < size - 1) {
        iRet = Read(&c, 1);
        if (iRet < 0) return -1;

        if (iRet != 0 && c != '\n') s[read_len++] = c;
        else break;
    }
    s[read_len] = '\0';
    return read_len;

}

/**
 * destructor
 *
 * @author          wangyida
 * @version                0.1.2
 * @param                s is the destination to store the line read
 * @func                Reads in characters from stream and stores them into the string s.
 Reading stops after an EOF or a newline.
 If a newline is read, it is stored into the buffer.
 * @return                return the number of bytes read, if an error occurs, return -1
 */

int64_t ReaderLocal::ReadLine(std::string& s) {
    s.clear();
    char c = 0;
    int iRet = 0;
    while (1) {
        iRet = Read(&c, 1);
        if (iRet < 0) return -1;

        if (iRet > 0 && c != '\n') s.push_back(c);
        else break;
    }
    return s.size();
}

/**
 * destructor
 *
 * @author          wangyida
 * @version                0.1.2
 * @param                see the declaration of this function
 * @func                if we use mmap, update the _read_ptr
 *                                 if we use direct read, we should recalculate the offset because there may be some data left in the buffer.
 * @return                return true if success, if an error occurs, return false.
 */

bool ReaderLocal::Seek(int64_t offset, int whence) {
    if (_map_buf == NULL) {
        if (!_direct_read) {
            off_t ret_off = lseek(_fd, offset, whence);
            if (ret_off == -1) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_SEEK, errno);
                return false;
            }
        } else {
            off_t real_seek_offset = offset;

            if (whence == SEEK_CUR) {
                real_seek_offset -= _left_data_size_in_buf;
            }
            _left_data_size_in_buf = 0;
            off_t ret_off = lseek(_fd, real_seek_offset, whence);
            if (ret_off == -1) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_SEEK, errno);
                return false;
            }
        }

    } else {
        switch (whence) {
            case SEEK_SET:
                _read_ptr = _map_buf + offset;
                break;
            case SEEK_CUR:
                _read_ptr += offset;
                break;
            case SEEK_END:
                _read_ptr = _map_buf + _file_size + offset;
                break;
            default:
                Err("the whence is wrong, please input the right value\n");
                GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                return false;
        }
    }

    return true;
}

NAMESPACE_END_SEISFS
}
