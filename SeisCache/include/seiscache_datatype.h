/*
 * seiscache_datatype.h
 *
 *  Created on: Apr 13, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_DATATYPE_H_
#define SEISCACHE_DATATYPE_H_


#include <string>
#include <vector>
#include <stdint.h>

namespace seisfs {

namespace cache {


#define HT_INT		0x0000
#define HT_UINT		0x0100
#define HT_FL		0x0200
#define HT_STR		0x0300

enum KeyType {
	TINYINT = 0x0001,
	SMALLINT = 0x0002,
	INT = 0x0004,  //int32_t
	BIGINT = 0x0008,  //int64_t

	UTINYINT = 0x0101,  //uint8_t
	USMALLINT = 0x0102,  //uint16_t
	UINT = 0x0104,  //uint32_t
	UBIGINT = 0x0108,  //uint64_t

	FLOAT = 0x0204,  //float
	DOUBLE = 0x0208  //double
};



struct HeadType {
	std::vector<int16_t> type_arr;

	int GetKeySize(int key_id) {
		return (type_arr[key_id] & 0x00ff);
	}
};

struct TraceType {
	uint32_t trace_size;
};

enum StoreState {
	STAT_STABLE,
	STAT_CACHED,
	STAT_BOTH
};

struct DataState {
	StoreState store_state;
	uid_t user_id;
	gid_t group_id;
};

}

}


#endif /* SEISCACHE_DATATYPE_H_ */
