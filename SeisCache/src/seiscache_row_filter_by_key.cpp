/*
 * seiscache_row_filter_by_key.cpp
 *
 *  Created on: Apr 14, 2018
 *      Author: wyd
 */


#include "seiscache_row_filter_by_key.h"

namespace seisfs {

namespace cache {

RowFilterByKey::RowFilterByKey() {

}
RowFilterByKey::~RowFilterByKey() {

}

RowFilterByKey& RowFilterByKey::FilterBy(const Key& key) {
	return *this;
}

}

}

