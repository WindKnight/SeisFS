/*
 * seisfile_typelist.h
 *
 *  Created on: Apr 23, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_TYPELIST_H_
#define SEISFILE_TYPELIST_H_

#include "seisfile.h"
#include "seisfile_nas.h"
#include "seisfile_hdfs.h"

#ifdef USE_GCACHE
#include "gcache_seis_data.h"
#endif

namespace seisfs {

namespace file {

#define SEISFILE_MAP(type_name)	\
	{ #type_name, &CreateObj<type_name>}


#ifdef USE_GCACHE

TypeMapping g_type_map[] = {
		SEISFILE_MAP(SeisFileNAS),
		SEISFILE_MAP(SeisFileHDFS),
		SEISFILE_MAP(HDFS)
};
#else
TypeMapping g_type_map[] = {
		SEISFILE_MAP(SeisFileNAS),
		SEISFILE_MAP(SeisFileHDFS)
};
#endif

int g_map_count = sizeof(g_type_map) / sizeof(g_type_map[0]);
}

}


#endif /* SEISFILE_TYPELIST_H_ */
