/*
 * seisfile.cpp
 *
 *  Created on: Apr 5, 2018
 *      Author: wyd
 */

#include "seisfile.h"
#include <map>
#include <algorithm>

#include "seisfile_typelist.h"
#include "util/seisfs_util_log.h"

namespace seisfs {

namespace file {

SeisFile* SeisFile::New(const std::string &data_name, StoreType store_type) {
	SeisFile *ret_file;
	switch (store_type) {
	case STABLE :
		ret_file = SeisFileReflectionManager::GetInstance(STABLE_FILE_TYPE, data_name);
		break;
	case CACHE:
		ret_file = SeisFileReflectionManager::GetInstance(CACHE_FILE_TYPE, data_name);
		break;
	default:
		ret_file = SeisFileReflectionManager::GetInstance(STABLE_FILE_TYPE, data_name);//TODO default filetype
		break;
	}

#if 0
	if(!ret_file->Init()) {
		return NULL;
	}
#endif
	return ret_file;
}

SeisFile::SeisFile() {

}

SeisFile::SeisFile(const std::string &data_name) :
	data_name_(data_name){

}
SeisFile::~SeisFile() {

}



void SeisFileReflectionManager::SetClasses(const TypeMapping mapping[], int map_num) {
	if(is_init_)
		return;
	std::transform(
			mapping, mapping + map_num,
			inserter(constructor_map_, constructor_map_.begin()),
			mem_fun_ref(&TypeMapping::MakePare));
	is_init_ = true;
}
SeisFile *SeisFileReflectionManager::GetInstance(const std::string &type_name, const std::string &data_name) {
	if(!is_init_) {
		SetClasses(g_type_map, g_map_count);
	}

	std::map<std::string, SeisFileConstructor>::iterator it= constructor_map_.find(type_name);
	if(it == constructor_map_.end()) {
		return NULL;
	}
	return it->second(data_name);

}

bool SeisFileReflectionManager::is_init_ = false;
std::map<std::string, SeisFileConstructor> SeisFileReflectionManager::constructor_map_;



}

}
