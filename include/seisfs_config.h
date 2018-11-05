/*
 * seisfs_config.h
 *
 *  Created on: Apr 23, 2018
 *      Author: wyd
 */

#ifndef SEISFS_CONFIG_H_
#define SEISFS_CONFIG_H_


namespace seisfs {

enum StoreType {
	AUTO = 0,
	STABLE = 1,
	CACHE = 2
};

//#define USE_GCACHE

#ifdef USE_GCACHE
#define HDFS_CLASS_NAME		"HDFS"
#else
#define HDFS_CLASS_NAME		"SeisFileHDFS"
#endif

#define STABLE_FILE_TYPE	"SeisFileNAS"
#define CACHE_FILE_TYPE		HDFS_CLASS_NAME




}

#endif /* SEISFS_CONFIG_H_ */
