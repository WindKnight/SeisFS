/*
 * seisfile_hdfs_metastruct.h
 *
 *  Created on: Sep 12, 2018
 *      Author: cssl
 */



namespace seisfs {

namespace file {

struct MetastructHDFS{
	int version;
	time_t creat_time;
	time_t lastmodify_time;
	time_t lastaccess_time;
	uint32_t head_num;
	uint32_t trace_num;

	uint32_t head_length;
	uint32_t trace_length;
	int keynum;
	Lifetime lifetime_days;
	LifetimeStat lifetimestat;
};

}
}

