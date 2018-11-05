/*
 * gcache_seis_helper.h
 *
 *  Created on: Jun 5, 2017
 *      Author: hadoop
 */

#ifndef SRC_SEIS_GCACHE_SEIS_HELPER_H_
#define SRC_SEIS_GCACHE_SEIS_HELPER_H_

#include "hdfs.h"
#include <iostream>


struct RichFileHandle {

    hdfsFile fd;             //file handle of file

    std::string file_name;   //the name of file

    int64_t access_time;     //the access time of file

};

#endif /* SRC_SEIS_GCACHE_SEIS_HELPER_H_ */
