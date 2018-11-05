/*
 * seiscache_head_speed_reader.h
 *
 *  Created on: Mar 29, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_HEAD_SPEED_READER_H_
#define SEISCACHE_HEAD_SPEED_READER_H_

#include "seiscache_speed_readerbase.h"

namespace seisfs {

namespace cache {

class HeadSpeedReader : public SpeedReaderBase{

public:
	HeadSpeedReader(const std::string data_name, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
	virtual ~HeadSpeedReader();

	bool Get(void *head);
private:


};

}
}

#endif /* SEISCACHE_HEAD_SPEED_READER_H_ */
