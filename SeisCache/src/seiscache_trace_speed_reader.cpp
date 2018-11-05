/*
 * seiscache_trace_speed_reader.cpp
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */


#include "seiscache_trace_speed_reader.h"
#include "seiscache_error.h"

namespace seisfs {

namespace cache {

TraceSpeedReader::TraceSpeedReader(const std::string data_name, StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
		SpeedReaderBase(data_name, real_store_type, meta_manager) {

}
TraceSpeedReader::~TraceSpeedReader() {

}

bool TraceSpeedReader::Get(void *trace) {
	ASSERT_CLOSED(false);
	return f_spd_reader_->ReadTrace((char*)trace);
}


}

}
