/*
 * seiscache_row_filter_by_key.h
 *
 *  Created on: Apr 12, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_ROW_FILTER_BY_KEY_H_
#define SEISCACHE_ROW_FILTER_BY_KEY_H_

#include "seiscache_key.h"

namespace seisfs {

namespace cache {

class RowFilterByKey {
public:
	RowFilterByKey();
	~RowFilterByKey();

	RowFilterByKey& FilterBy(const Key& key);

private:

};

}

}


#endif /* SEISCACHE_ROW_FILTER_BY_KEY_H_ */
