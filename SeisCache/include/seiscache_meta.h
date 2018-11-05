/*
 * seiscache_meta.h
 *
 *  Created on: Mar 22, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_META_H_
#define SEISCACHE_META_H_

#include <string>
#include "seisfs_meta.h"
#include "seisfs_config.h"
#include "seiscache_datatype.h"

#include "data_meta.h"
#include "glock_mutex.h"

#define LAST_ACC	0
#define LAST_MOD	1


#define MOD_NONE	0x00
#define MOD_HEAD	0x01
#define MOD_TRACE	0x02

namespace seisfs {

namespace cache {

#if 0

typedef struct {
	int version;
	int updating_num;
	time_t last_accessed;
	time_t last_modified;
	bool is_writing;

public:
	inline void Clear() {
		is_writing = false;
		updating_num = 0;
	}
} SeisCachMeta;
#endif


class MetaManager {
public:
	MetaManager(const std::string& data_name);
	~MetaManager();

	bool InitAllMeta();

	bool IsValid(StoreType store_type);
	bool SetLifetimeStat(StoreType store_type, LifetimeStat stat);

	bool SetWriting();
	bool ClearWriting();
	bool GetWritingNum(int &wrting_num);

	bool SetUpdating();
	bool ClearUpdating();
	bool ClearAllUpdating();
	bool GetUpdatingNum(int &updating_num);

	bool RecordLastAccessed();
	time_t GetLastAccessed();
	bool RecordLastModified();
	time_t GetLastModified();

	bool GetHeadType(HeadType *head_type);
	bool GetTraceType(TraceType *trace_type);
	bool SaveHeadType(const HeadType &head_type);
	bool SaveTraceType(const TraceType &trace_type);

	bool TryLockWriteHead();
	void UnlockWriteHead();

	bool TryLockWriteTrace();
	void UnlockWriteTrace();

	bool TryLockWrite();
	void UnlockWrite();

	bool SetWriteBackFlag(int mod_type);
	bool ClearWriteBackFlag(int mod_type);
	bool GetWriteBackFlag(int mod_type, bool &in_queue);

	bool Remove();
	bool CopyMeta(const std::string &new_name);

private:

	bool PutMeta(SeisMeta *seis_meta, char *buf, int size);
	bool GetMeta(SeisMeta *seis_meta, char *buf, int size);

	bool RecordTime(time_t time, int time_type);
	time_t GetTime(int time_type);

	std::string GetVersionMetaName(); //TODO is this useful? when should we set it?
	std::string GetWritingMetaName();
	std::string GetUpdatingMetaName();
	std::string GetLastAccMetaName();
	std::string GetLastModMetaName();
	std::string GetHeadTypeMetaName();
	std::string GetHeadTypeNumMetaName();
	std::string GetTraceTypeMetaName();
	std::string GetWriteBackFlagName();

	std::string GetWriteMutexHeadName();
	std::string GetWriteMutexTraceName();
	std::string GetUpdateMetaMutexName();

	std::string data_name_;
};

}

}


#endif /* SEISCACHE_META_H_ */
