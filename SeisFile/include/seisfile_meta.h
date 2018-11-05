/*
 * seisfile_meta.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_META_H_
#define SEISFILE_META_H_


#include "seisfs_meta.h"
#include <vector>

#include <stddef.h>

namespace seisfs {

namespace file {

#define LIFETIME_NULL 		-1
#define LIFETIME_DEFAULT 	30

enum HeadPlacementStruct {
    BY_ROW = 0, BY_COLUMN = 1,
};

/*
 * only recommended. child class implementers could use any meta structure as they want.
 */
#if 0
struct MetaSeis {
    struct MetaHeader meta;
    int version;
    uint32_t head_length;
    uint32_t trace_length;
    HeadPlacementStruct head_placement;
    time_t creation_time;

    int* head_sizes; // an array that indicates the number of bytes for each head field

    MetaSeis() {
        meta.lifetime = 0;
        meta.tag = NORMAL;
        version = 0;
        head_length = 0;
        head_placement = BY_ROW;
        trace_length = 0;
        creation_time = 0;

        head_sizes = NULL;

    }

    ~MetaSeis() {
    	if(NULL != head_sizes) {
            delete head_sizes;
            head_sizes = NULL;
    	}


    }
};
#endif

struct HeadType {
	std::vector<uint16_t> head_size;
};

struct TraceType {
    uint32_t trace_size;
};


typedef int SeisKey;
typedef std::vector<SeisKey> SortKeys;

typedef int Lifetime;



}

}

#endif /* SEISFILE_META_H_ */
