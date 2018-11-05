/*
 * utils.h
 *
 *  Created on: Jun 12, 2016
 *      Author: neo
 */

#ifndef SRC_TMP_HDFS_HDFS_UTILS_UTILS_H_
#define SRC_TMP_HDFS_HDFS_UTILS_UTILS_H_

#include <string>
#include <hdfs.h>
#include <unistd.h>
#include "gcache_global.h"
#include "gcache_seis_error.h"

hdfsFS getHDFS(std::string path);

char* hdfsFgets(char * buffer, int size, hdfsFS fs, hdfsFile fp);

std::string hdfsGetPath(std::string path);

int hdfsTruncateWait(hdfsFS fs, const char *path, tOffset pos);

#endif /* SRC_TMP_HDFS_HDFS_UTILS_UTILS_H_ */
