/*
 * seisfs_util_io.h
 *
 *  Created on: Apr 24, 2016
 *      Author: wyd
 */

#ifndef SEISFS_UTIL_IO_H_
#define SEISFS_UTIL_IO_H_

#include <sys/types.h>
#include <string>


namespace seisfs {

bool RecursiveRemove(const std::string &path);

bool RecursiveMakeDir(const std::string &dir_path);

int64_t SafeWrite(int fd, const char *buf, int64_t size) ;

int64_t SafeRead(int fd, char *buf, int64_t size);

bool FloatEqual(float a, float b);

}


#endif /* SEISFS_UTIL_IO_H_ */
