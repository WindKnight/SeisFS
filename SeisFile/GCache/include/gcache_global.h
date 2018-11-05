/*
 * gcache_global.h
 *
 *  Created on: Jan 12, 2016
 *      Author: wyd
 */

#ifndef GCACHE_GLOBAL_H_
#define GCACHE_GLOBAL_H_

#define NAMESPACE_BEGIN_SEISFS namespace seisfs {
#define NAMESPACE_END_SEISFS        }

#ifndef INT64_MAX
#define INT64_MAX 9223372036854775807
#endif

#define MAX_UPDATED_ROW 2000000  //for auto compact
#define REPLICATION 1
#define UUID_LENGTH 37
#define TRUN_ROW_SIZE 40
#define CACHE_FD_SIZE 20  //rich_fd_handle maximum number in memory
#define MAX_TRUNCATE_WAIT 20    //20 seconds

#define GCACHE_DISABLE_COPY(Class) \
    Class(const Class &); \
    Class &operator=(const Class &);

#endif /* GCACHE_GLOBAL_H_ */
