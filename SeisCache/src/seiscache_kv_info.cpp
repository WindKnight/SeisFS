/*
 * seiscache_kv_info.cpp
 *
 *  Created on: Apr 14, 2018
 *      Author: wyd
 */


#include "seiscache_kv_info.h"
#include "seisfile_kvinfo.h"

namespace seisfs {

namespace cache {

KVInfo::KVInfo(SharedPtr<file::KVInfo> f_kvinfo, StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
	f_kvinfo_(f_kvinfo), real_store_type_(real_store_type), meta_manager_(meta_manager){

}

KVInfo::~KVInfo() {

}

bool KVInfo::Put(const std::string &key, const char* data, uint32_t size) {
	if(!f_kvinfo_->Put(key, data, size)) {
		return false;
	}

	if(!meta_manager_->RecordLastModified()) {
		return false;
	}
	return true;
}

bool KVInfo::Delete(const std::string &key) {
	if(!f_kvinfo_->Delete(key)) {
		return false;
	}

	if(!meta_manager_->RecordLastModified()) {
		return false;
	}
	return true;
}

bool KVInfo::Get(const std::string &key, char*& data, uint32_t &size) {
	if(!f_kvinfo_->Get(key, data, size)) {
		return false;
	}

	if(!meta_manager_->RecordLastAccessed()) {
		return false;
	}
	return true;
}

int32_t KVInfo::SizeOf(const std::string &key) {
	int ret = f_kvinfo_->SizeOf(key);

	if(!meta_manager_->RecordLastAccessed()) {
		return -1;
	}
	return ret;
}

bool KVInfo::Exists(const std::string &key) {
	bool ret = f_kvinfo_->Exists(key);

	if(!meta_manager_->RecordLastAccessed()) {
		return false;
	}
	return ret;
}

bool KVInfo::GetKeys(std::vector<std::string> &Keys) {
	if(!f_kvinfo_->GetKeys(Keys)) {
		return false;
	}

	if(!meta_manager_->RecordLastAccessed()) {
		return false;
	}
	return true;
}


}
}

