/*
 * seiscache_speed_reader.h
 *
 *  Created on: Mar 29, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_SPEED_READER_H_
#define SEISCACHE_SPEED_READER_H_

#include "seiscache_speed_readerbase.h"

namespace seisfs {

namespace cache {

class SpeedReader : public SpeedReaderBase{

public:
	SpeedReader(const std::string data_name, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
	virtual ~SpeedReader();

	bool Get(void *head, void *trace);
};

}

}



#endif /* SEISCACHE_SPEED_READER_H_ */
