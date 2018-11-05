/*
 * seiscache_trace_speed_reader.h
 *
 *  Created on: Mar 29, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_TRACE_SPEED_READER_H_
#define SEISCACHE_TRACE_SPEED_READER_H_

#include "seiscache_speed_readerbase.h"
namespace seisfs {

namespace cache {

class TraceSpeedReader : public SpeedReaderBase{

public:
	TraceSpeedReader(const std::string data_name, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
	virtual ~TraceSpeedReader();

	bool Get(void *trace);

};

}

}

#endif /* SEISCACHE_TRACE_SPEED_READER_H_ */
