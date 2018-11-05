/*
 * seiscache_kv_info.h
 *
 *  Created on: Mar 22, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_KV_INFO_H_
#define SEISCACHE_KV_INFO_H_

#include <string>
#include <vector>
#include <stdint.h>
#include "util/seisfs_shared_pointer.h"
#include "seisfile_kvinfo.h"
#include "seiscache_meta.h"

namespace seisfs {

namespace cache {

/*
 * A key-value storage to record extra information about seismic data
 */

class KVInfo {
public:
	KVInfo(SharedPtr<file::KVInfo> f_kvinfo, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);

    ~KVInfo();

    bool Put(const std::string &key, const char* data, uint32_t size);

    bool Delete(const std::string &key);

    bool Get(const std::string &key, char*& data, uint32_t &size);

    int32_t SizeOf(const std::string &key);

    bool Exists(const std::string &key);

    bool GetKeys(std::vector<std::string> &Keys);

private:

    SharedPtr<file::KVInfo> f_kvinfo_;
    SharedPtr<MetaManager> meta_manager_;
    StoreType real_store_type_;

};

//there is no need to use the reelmerge in gcache. because the only user of gcache is seiscache, and seiscache doesn't use reelmerge.

class KVInfoMerger {
public:
	KVInfoMerger() {}
	virtual ~KVInfoMerger() {}

	/*
	 * merge extra information of info_0 and info_1 into info_0
	 */
	virtual bool Merge(KVInfo *info_0, KVInfo *info_1) = 0;
};

}

}


#endif /* SEISCACHE_KV_INFO_H_ */
