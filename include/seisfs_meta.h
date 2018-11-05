/*
 * seisfs_meta.h
 *
 *  Created on: Apr 5, 2018
 *      Author: wyd
 */

#ifndef SEISFS_META_H_
#define SEISFS_META_H_

#include <stdint.h>

namespace seisfs {


enum LifetimeStat {
	NORMAL = 0,
	MOVE_TO_STABLE = 1,
	EXPIRED = 2,  //expired but still could be read
	INVALID = 3 //invalid data, could not be read, should be deleted.
};

struct MetaHeader {
	LifetimeStat tag;
    int16_t lifetime;

};



}



#endif /* SEISFS_META_H_ */
