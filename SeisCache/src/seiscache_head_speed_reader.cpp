/*
 * seiscache_head_speed_reader.cpp
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */

#include "seiscache_head_speed_reader.h"
#include "seiscache_error.h"
#include "util/seisfs_util_log.h"

namespace seisfs {
namespace cache{

HeadSpeedReader::HeadSpeedReader(const std::string data_name, StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
	SpeedReaderBase(data_name, real_store_type, meta_manager){

}
HeadSpeedReader::~HeadSpeedReader() {

}

bool HeadSpeedReader::Get(void *head) {
	ASSERT_CLOSED(false);
	return f_spd_reader_->ReadHead((char*)head);
}

}
}


