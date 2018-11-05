/*
 * seiscache_data.cpp
 *
 *  Created on: Apr 13, 2018
 *      Author: wyd
 */

#include "seiscache_data.h"
#include "seiscache_key.h"
#include "seiscache_row_filter_by_key.h"
#include "seiscache_meta.h"

#include "seiscache_writer.h"
#include "seiscache_trace_writer.h"
#include "seiscache_head_writer.h"

#include "seiscache_reader.h"
#include "seiscache_trace_reader.h"
#include "seiscache_head_reader.h"

#include "seiscache_speed_reader.h"
#include "seiscache_head_speed_reader.h"
#include "seiscache_trace_speed_reader.h"

#include "seiscache_updater.h"
#include "seiscache_trace_updater.h"
#include "seiscache_head_updater.h"

#include "seiscache_kv_info.h"

#include "seisfile.h"
//#include "seisfile_writer.h"
//#include "seisfile_headwriter.h"
//#include "seisfile_tracewriter.h"

#include "seiscache_error.h"

#include "seisfs_row_filter.h"
#include "util/seisfs_scoped_pointer.h"
#include "util/seisfs_util_log.h"

namespace seisfs {


namespace cache {

SeisCache::SeisCache(const std::string& data_name) :
		data_name_(data_name) {
	meta_manager_ = new MetaManager(data_name_);
}

SeisCache::~SeisCache() {

}

SeisCache::SeisCache(const SeisCache& data) {
	data_name_ = data.data_name_;
	meta_manager_ = new MetaManager(data_name_);
}

SeisCache& SeisCache::operator= (const SeisCache& other) {
	data_name_ = other.data_name_;
	meta_manager_ = new MetaManager(data_name_);
	return *this;
}

/**
 * ExtInfo of data. Key-value store.
 */
KVInfo* SeisCache::OpenKVInfo(StoreType store_type) {
	StoreType real_store_type;

	if(this->Exists()) {
		if(!ChooseReadStoreType(store_type, real_store_type)) {
			return NULL;
		}

	} else {
		if(store_type == AUTO) {
			real_store_type = CACHE;
		} else {
			real_store_type = store_type;
		}
	}

	ScopedPointer<file::SeisFile> seis_file(file::SeisFile::New(data_name_, real_store_type));
	ASSERT_NEW(seis_file.data(), NULL);

	if(!seis_file->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return NULL;
	}

	SharedPtr<file::KVInfo> f_kvinfo = seis_file->OpenKVInfo();
	KVInfo *kvinfo_ret = new KVInfo(f_kvinfo, real_store_type, meta_manager_);
	ASSERT_NEW(kvinfo_ret, NULL);

	return kvinfo_ret;
}

/*
 * Read only.
 * use head_filter to choose keys that you want to read
 * use row_filter to choose traces that you want to read
 * use storage_type to choose the seis data source, for example ,from nas, hdfs. default is automatic, which means seiscache will choose
 * the appropriate seis data automatically.
 */
bool SeisCache::CheckDataIntegrity() {
	if(meta_manager_->IsValid(STABLE) && meta_manager_->IsValid(CACHE)) {

		ScopedPointer<file::SeisFile> seis_file_stable(file::SeisFile::New(data_name_, STABLE));
		ASSERT_NEW(seis_file_stable, false);

		if(!seis_file_stable->Init()) {
			sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
			return false;
		}

		ScopedPointer<file::SeisFile> seis_file_cache(file::SeisFile::New(data_name_, CACHE));
		ASSERT_NEW(seis_file_cache, false);

		if(!seis_file_cache->Init()) {
			sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
			return false;
		}

		int64_t trace_num_stable = seis_file_stable->GetTraceNum();
		int64_t trace_num_cache = seis_file_cache->GetTraceNum();

		if(trace_num_stable != trace_num_cache) {
			sc_errno = SC_ERR_INTEGRITY;
			return false;
		}
	}

	return true;
}

bool SeisCache::ChooseReadStoreType(StoreType store_type, StoreType &real_store_type) {

	bool is_stable_valid = meta_manager_->IsValid(STABLE);
	bool is_cache_valid = meta_manager_->IsValid(CACHE);

//	Reader *reader;

	if(store_type == STABLE) {
		if(!is_stable_valid) {
			sc_errno = SC_ERR_NOVAL;
			return false;
		} else {
			//read from nas
			real_store_type = STABLE;
		}
	} else if(store_type == CACHE) {
		if(!is_cache_valid) {
			sc_errno = SC_ERR_NOVAL;
			return false;
		} else {
			//read from hdfs
			real_store_type = CACHE;
		}
	} else {
		if(is_cache_valid) {
			//read from hdfs
			real_store_type = CACHE;

		} else if(is_stable_valid) {
			//read from nas
			real_store_type = STABLE;

		} else {
			sc_errno = SC_ERR_NOVAL;
			return false;
		}
	}

	return true;
}

ReaderBase *SeisCache::OpenReaders(StoreType real_store_type, OpenType open_type,
		SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilter> row_filter) {

	ScopedPointer<file::SeisFile> seis_file(file::SeisFile::New(data_name_, real_store_type));
	ASSERT_NEW(seis_file.data(), NULL);
	if(!seis_file->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return NULL;
	}

	SharedPtr<file::Reader> f_reader = NULL;
	SharedPtr<file::TraceReader> f_trace_reader = NULL;
	SharedPtr<file::HeadReader> f_head_reader = NULL;

	ReaderBase *reader_ret;

	switch(open_type) {
	case OPEN_HEAD :
		f_head_reader = seis_file->OpenHeadReader();
		if(NULL == f_head_reader) {
			sc_errno = SCCombineErrno(SC_ERR_OP_FRD, errno);
			return NULL;
		}
		reader_ret = new HeadReader(f_head_reader, real_store_type, meta_manager_);
		ASSERT_NEW(reader_ret, NULL);

		ASSERT(reader_ret->SetHeadFilter(head_filter), NULL);
		ASSERT(reader_ret->SetRowFilter(row_filter), NULL);
		break;
	case OPEN_TRACE :
		f_trace_reader = seis_file->OpenTraceReader();
		if(NULL == f_trace_reader) {
			sc_errno = SCCombineErrno(SC_ERR_OP_FRD, errno);
			return NULL;
		}
		reader_ret = new TraceReader(f_trace_reader, real_store_type, meta_manager_);
		ASSERT_NEW(reader_ret, NULL);
		ASSERT(reader_ret->SetRowFilter(row_filter), NULL);
		break;
	case OPEN_WHOLE :
		f_reader = seis_file->OpenReader();
		if(NULL == f_reader) {
			sc_errno = SCCombineErrno(SC_ERR_OP_FRD, errno);
			return NULL;
		}
		reader_ret = new Reader(f_reader, real_store_type, meta_manager_);
		ASSERT_NEW(reader_ret, NULL);
		ASSERT(reader_ret->SetHeadFilter(head_filter), NULL);
		ASSERT(reader_ret->SetRowFilter(row_filter), NULL);
		break;
	default:
		reader_ret = NULL;
		break;
	}

	return reader_ret;
}


HeadReader*  SeisCache::OpenHeadReader(SharedPtr<HeadFilter> head_filter,
		SharedPtr<RowFilter> row_filter, StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return NULL;
//	}

	StoreType real_store_type;
	if(!ChooseReadStoreType(store_type, real_store_type)) {
		return NULL;
	}
//	StoreType real_store_type = CACHE;
	OpenType open_type = OPEN_HEAD;
	HeadReader *reader_ret = (HeadReader*)OpenReaders(real_store_type, open_type, head_filter, row_filter);
	return reader_ret;

}

TraceReader* SeisCache::OpenTraceReader(SharedPtr<RowFilter> row_filter,
		StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return NULL;
//	}

	StoreType real_store_type;
	if(!ChooseReadStoreType(store_type, real_store_type)) {
		return NULL;
	}

	OpenType open_type = OPEN_TRACE;

	TraceReader *reader_ret = (TraceReader*)OpenReaders(real_store_type, open_type, NULL, row_filter);
	return reader_ret;
}

Reader* SeisCache::OpenReader(SharedPtr<HeadFilter> head_filter,
		SharedPtr<RowFilter> row_filter, StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return NULL;
//	}

	StoreType real_store_type;
	if(!ChooseReadStoreType(store_type, real_store_type)) {
		return NULL;
	}

	OpenType open_type = OPEN_WHOLE;

	Reader *reader_ret = (Reader*)OpenReaders(real_store_type, open_type, head_filter, row_filter);
	return reader_ret;
}

/*
 * used to accelerate random read. if use HDFS, this will launch the distributed read process. If use NAS, this will launch a Speed read.
 */
HeadSpeedReader *SeisCache::OpenHeadSpeedReader(SharedPtr<HeadFilter> head_filter,
		SharedPtr<RowFilter> row_filter, StoreType store_type) {
#if 1
//	if(!CheckDataIntegrity()) {
//		return NULL;
//	}

	StoreType real_store_type;
	if(!ChooseReadStoreType(store_type, real_store_type)) {
		return NULL;
	}
#else
	StoreType real_store_type = store_type;
#endif

	HeadSpeedReader *ret = new HeadSpeedReader(data_name_, real_store_type, meta_manager_);
	ASSERT_NEW(ret, NULL);
	if(!ret->Init()) {
		return NULL;
	}

	ASSERT(ret->SetHeadFilter(head_filter), NULL);
	ASSERT(ret->SetRowFilter(row_filter), NULL);
	return ret;
}

TraceSpeedReader *SeisCache::OpenTraceSpeedReader(SharedPtr<RowFilter> row_filter,
		StoreType store_type) {
#if 1
//	if(!CheckDataIntegrity()) {
//		return NULL;
//	}

	StoreType real_store_type;
	if(!ChooseReadStoreType(store_type, real_store_type)) {
		return NULL;
	}
#else
	StoreType real_store_type = store_type;
#endif

	TraceSpeedReader *ret = new TraceSpeedReader(data_name_, real_store_type, meta_manager_);
	ASSERT_NEW(ret, NULL);
	if(!ret->Init()) {
		return NULL;
	}

	ASSERT(ret->SetRowFilter(row_filter), NULL);
	return ret;
}

SpeedReader *SeisCache::OpenSpeedReader(SharedPtr<HeadFilter> head_filter,
		SharedPtr<RowFilter> row_filter, StoreType store_type) {

#if 1
//	if(!CheckDataIntegrity()) {
//		return NULL;
//	}

	StoreType real_store_type;
	if(!ChooseReadStoreType(store_type, real_store_type)) {
		return NULL;
	}
#else
	StoreType real_store_type = store_type;
#endif

	SpeedReader *ret = new SpeedReader(data_name_, real_store_type, meta_manager_);
	ASSERT_NEW(ret, NULL);
	if(!ret->Init()) {
		return NULL;
	}

	ASSERT(ret->SetHeadFilter(head_filter), NULL);
	ASSERT(ret->SetRowFilter(row_filter), NULL);
	return ret;
}

/*
 * Append only
 * head type is used to set the key number and size of each key
 * trace type is used to set length of one trace.
 */


WriterBase *SeisCache::OpenWriters(StoreType real_store_type, OpenType open_type, bool open_new,
		SharedPtr<HeadType> head_type, SharedPtr<TraceType> trace_type) {

	WriterBase *writer_ret = NULL;

	ScopedPointer<file::SeisFile> seis_file(file::SeisFile::New(data_name_, real_store_type));
	ASSERT_NEW(seis_file.data(), NULL);
	if(!seis_file->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return NULL;
	}

	SharedPtr<file::Writer> f_writer = NULL;
	SharedPtr<file::TraceWriter> f_trace_writer = NULL;
	SharedPtr<file::HeadWriter> f_head_writer = NULL;


	if(open_new) {
		file::HeadType f_head_type;
		file::TraceType f_trace_type;

		for(int i = 0; i < head_type->type_arr.size(); ++i) {
			f_head_type.head_size.push_back((head_type->type_arr[i] & 0x00ff));
		}

		if(real_store_type == CACHE) {

			f_trace_type.trace_size = trace_type->trace_size / 4; //TODO
			printf("f_trace_type.trace_size = %d\n", f_trace_type.trace_size);
		} else {
			f_trace_type.trace_size = trace_type->trace_size;
		}


		switch(open_type) {
		case OPEN_HEAD :
			f_head_writer = seis_file->OpenHeadWriter(f_head_type);
			if(NULL == f_head_writer) {
				sc_errno = SCCombineErrno(SC_ERR_OP_FWR, errno);
				return NULL;
			}
			writer_ret = new HeadWriter(data_name_, f_head_writer, real_store_type, meta_manager_);
			ASSERT_NEW(writer_ret, NULL);
			break;
		case OPEN_TRACE :
			f_trace_writer = seis_file->OpenTraceWriter(f_trace_type);
			if(NULL == f_trace_writer) {
				sc_errno = SCCombineErrno(SC_ERR_OP_FWR, errno);
				return NULL;
			}
			writer_ret = new TraceWriter(data_name_, f_trace_writer, real_store_type, meta_manager_);
			ASSERT_NEW(writer_ret, NULL);
			break;
		case OPEN_WHOLE :
			f_writer = seis_file->OpenWriter(f_head_type, f_trace_type);
			if(NULL == f_writer) {
				sc_errno = SCCombineErrno(SC_ERR_OP_FWR, errno);
				return NULL;
			}
			writer_ret = new Writer(data_name_, f_writer, real_store_type, meta_manager_);
			ASSERT_NEW(writer_ret, NULL);
			break;
		default:
			writer_ret = NULL;
			break;
		}

		if(!meta_manager_->InitAllMeta()) {
			return NULL;
		}

		if(!meta_manager_->SaveHeadType(*head_type)) {
			return NULL;
		}
		if(!meta_manager_->SaveTraceType(*trace_type)) {
			return NULL;
		}

		meta_manager_->SetLifetimeStat(real_store_type, NORMAL);
	} else {
		switch(open_type) {
		case OPEN_HEAD :
			f_head_writer = seis_file->OpenHeadWriter();
			if(NULL == f_head_writer) {
				sc_errno = SCCombineErrno(SC_ERR_OP_FWR, errno);
				return NULL;
			}
			writer_ret = new HeadWriter(data_name_, f_head_writer, real_store_type, meta_manager_);
			ASSERT_NEW(writer_ret, NULL);
			break;
		case OPEN_TRACE :
			f_trace_writer = seis_file->OpenTraceWriter();
			if(NULL == f_trace_writer) {
				sc_errno = SCCombineErrno(SC_ERR_OP_FWR, errno);
				return NULL;
			}
			writer_ret = new TraceWriter(data_name_, f_trace_writer, real_store_type, meta_manager_);
			ASSERT_NEW(writer_ret, NULL);
			break;
		case OPEN_WHOLE :
			f_writer = seis_file->OpenWriter();
			if(NULL == f_writer) {
				sc_errno = SCCombineErrno(SC_ERR_OP_FWR, errno);
				return NULL;
			}
			writer_ret = new Writer(data_name_, f_writer, real_store_type, meta_manager_);
			ASSERT_NEW(writer_ret, NULL);
			break;
		default:
			writer_ret = NULL;
			break;
		}
	}

	return writer_ret;
}

bool SeisCache::ChooseWriterStoreType(StoreType store_type, StoreType &real_store_type, bool &open_new) {


	bool is_stable_valid = meta_manager_->IsValid(STABLE);
	bool is_cache_valid = meta_manager_->IsValid(CACHE);
	if(store_type == CACHE) {

		real_store_type = CACHE;

		if(!is_cache_valid) {
			if(is_stable_valid) {
				sc_errno = SC_ERR_OP_INVAL_WR;
				return false;
			} else {
				open_new = true;
			}

		} else {
			open_new = false;
		}

	} else if(store_type == STABLE) {
		real_store_type = STABLE;

		if(!is_stable_valid) {
			if(is_cache_valid) {
				sc_errno = SC_ERR_OP_INVAL_WR;
				return false;
			} else {
				open_new = true;
			}
		} else {
			open_new = false;
		}

	} else {

			if(is_stable_valid && (!is_cache_valid)) { //write to exist nas
				real_store_type = STABLE;
				open_new = false;

			} else { // use hdfs
				real_store_type = CACHE;
				if(!is_cache_valid) { // if this is a new data, have to generate a new reel header
					open_new = true;
				} else {
					open_new = false;
				}
			}
	}

	return true;
}


HeadWriter*  SeisCache::OpenHeadWriter(SharedPtr<HeadType> head_type, StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return false;
//	}

#if 0
	if(meta_manager_->IsUpdating()) {
//		seis_errno = SeisbaseCombineErrno(SEIS_ERR_UPWR, errno);
		return false;
	}
#endif
	//TODO if open writer when is updating, this may lead to removing all index, and update will fail.

	if(!meta_manager_->TryLockWriteHead()) {
		return NULL;
	}

	OpenType open_type = OPEN_HEAD;
	bool open_new = false;
	StoreType real_store_type;

	if(!ChooseWriterStoreType(store_type, real_store_type, open_new)) {
		return NULL;
	}

	HeadWriter *writer_ret = (HeadWriter*)OpenWriters(real_store_type, open_type, open_new, head_type, NULL);
	if(NULL == writer_ret) {
		meta_manager_->UnlockWriteHead();
	} else {
		if(!meta_manager_->SetWriting()) {
//			seis_errno = SeisbaseCombineErrno(SEIS_ERR_RECMETA, errno);
			meta_manager_->UnlockWriteHead();
			delete writer_ret;
			writer_ret = NULL;
		}
	}
	return writer_ret;
}

TraceWriter* SeisCache::OpenTraceWriter(SharedPtr<TraceType> trace_type, StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return false;
//	}

#if 0
	if(meta_manager_->IsUpdating()) {
//		seis_errno = SeisbaseCombineErrno(SEIS_ERR_UPWR, errno);
		return false;
	}
#endif
	//TODO if open writer when is updating, this may lead to removing all index, and update will fail.

	if(!meta_manager_->TryLockWriteTrace()) {
		return NULL;
	}

	OpenType open_type = OPEN_TRACE;
	bool open_new = false;
	StoreType real_store_type;

	if(!ChooseWriterStoreType(store_type, real_store_type, open_new)) {
		return NULL;
	}

	TraceWriter *writer_ret = (TraceWriter*)OpenWriters(real_store_type, open_type, open_new, NULL, trace_type);
	if(NULL == writer_ret) {
		meta_manager_->UnlockWriteTrace();
	} else {
		if(!meta_manager_->SetWriting()) {
			meta_manager_->UnlockWriteTrace();
			delete writer_ret;
			writer_ret = NULL;
		}
	}
	return writer_ret;
}


Writer* SeisCache::OpenWriter(SharedPtr<HeadType> head_type, SharedPtr<TraceType> trace_type, StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return false;
//	}
#if 0
	if(meta_manager_->IsUpdating()) {
//		seis_errno = SeisbaseCombineErrno(SEIS_ERR_UPWR, errno);
		return false;
	}
#endif
	//TODO if open writer when is updating, this may lead to removing all index, and update will fail.

	if(!meta_manager_->TryLockWrite()) {
		return NULL;
	}

	OpenType open_type = OPEN_WHOLE;
	bool open_new = false;
	StoreType real_store_type;
	if(!ChooseWriterStoreType(store_type, real_store_type, open_new)) {
		return NULL;
	}
	Writer *writer_ret = (Writer*)OpenWriters(real_store_type, open_type, open_new, head_type, trace_type);
	if(NULL == writer_ret) {
		meta_manager_->UnlockWrite();
	} else {
		if(!meta_manager_->SetWriting()) {
			meta_manager_->UnlockWrite();
			delete writer_ret;
			writer_ret = NULL;
		}
	}
	return writer_ret;
}


/*
 * update only
 */


UpdaterBase *SeisCache::OpenUpdaters(StoreType real_store_type, OpenType open_type,
		SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilter> row_filter) {

	SharedPtr<file::SeisFile> seis_file = file::SeisFile::New(data_name_, real_store_type);
	ASSERT_NEW(seis_file, NULL);
	if(!seis_file->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return NULL;
	}

	SharedPtr<file::TraceUpdater> f_trace_updater = NULL;
	SharedPtr<file::HeadUpdater> f_head_updater = NULL;

	UpdaterBase *updater_ret;

	switch(open_type) {
	case OPEN_HEAD :
		f_head_updater = seis_file->OpenHeadUpdater();
		if(NULL == f_head_updater) {
			sc_errno = SCCombineErrno(SC_ERR_OP_FUP, errno);
			return NULL;
		}
		updater_ret = new HeadUpdater(data_name_, f_head_updater, seis_file, real_store_type, meta_manager_);
		ASSERT_NEW(updater_ret, NULL);
		ASSERT(updater_ret->SetHeadFilter(head_filter), NULL);
		ASSERT(updater_ret->SetRowFilter(row_filter), NULL);
		break;
	case OPEN_TRACE :
		f_trace_updater = seis_file->OpenTraceUpdater();
		if(NULL == f_trace_updater) {
			sc_errno = SCCombineErrno(SC_ERR_OP_FUP, errno);
			return NULL;
		}
		updater_ret = new TraceUpdater(data_name_, f_trace_updater, seis_file, real_store_type, meta_manager_);
		ASSERT_NEW(updater_ret, NULL);
		ASSERT(updater_ret->SetRowFilter(row_filter), NULL);
		break;
	case OPEN_WHOLE :
		f_head_updater = seis_file->OpenHeadUpdater();
		if(NULL == f_head_updater) {
			sc_errno = SCCombineErrno(SC_ERR_OP_FUP, errno);
			return NULL;
		}
		f_trace_updater = seis_file->OpenTraceUpdater();
		if(NULL == f_trace_updater) {
			sc_errno = SCCombineErrno(SC_ERR_OP_FUP, errno);
			return NULL;
		}
		updater_ret = new Updater(data_name_, f_head_updater, f_trace_updater, seis_file, real_store_type, meta_manager_);
		ASSERT_NEW(updater_ret, NULL);
		ASSERT(updater_ret->SetHeadFilter(head_filter), NULL);
		ASSERT(updater_ret->SetRowFilter(row_filter), NULL);
		break;
	default:
		updater_ret = NULL;
		break;
	}

	return updater_ret;
}

HeadUpdater* SeisCache::OpenHeadUpdater(SharedPtr<HeadFilter> head_filter,
		SharedPtr<RowFilter> row_filter, StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return false;
//	}

#if 0
	if(seis_meta_->IsWriting()) {
		seis_errno = SeisbaseCombineErrno(SEIS_ERR_WRUP, errno);
		return NULL;
	}
#endif
	//TODO the data should be able to be updated when it is writing, right?

	StoreType real_store_type;

	if(!ChooseReadStoreType(store_type,real_store_type)) {
		return NULL;
	}

	OpenType open_type = OPEN_HEAD;

	HeadUpdater *updater_ret = (HeadUpdater *)OpenUpdaters(real_store_type, open_type, head_filter, row_filter);
	if(NULL == updater_ret) {
		return NULL;
	} else {
		if(!meta_manager_->SetUpdating()) {
			delete updater_ret;
			return NULL;
		}
	}

	return updater_ret;
}

TraceUpdater* SeisCache::OpenTraceUpdater(SharedPtr<RowFilter> row_filter,
		StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return false;
//	}

#if 0
	if(seis_meta_->IsWriting()) {
		seis_errno = SeisbaseCombineErrno(SEIS_ERR_WRUP, errno);
		return NULL;
	}
#endif
	//TODO the data should be able to be updated when it is writing, right?

	StoreType real_store_type;

	if(!ChooseReadStoreType(store_type,real_store_type)) {
		return NULL;
	}

	OpenType open_type = OPEN_TRACE;

	TraceUpdater *updater_ret = (TraceUpdater *)OpenUpdaters(real_store_type, open_type, NULL, row_filter);
	if(NULL == updater_ret) {
		return NULL;
	} else {
		if(!meta_manager_->SetUpdating()) {
			delete updater_ret;
			return NULL;
		}
	}

	return updater_ret;
}

Updater* SeisCache::OpenUpdater(SharedPtr<HeadFilter> head_filter,
		SharedPtr<RowFilter> row_filter, StoreType store_type) {

//	if(!CheckDataIntegrity()) {
//		return false;
//	}

#if 0
	if(seis_meta_->IsWriting()) {
		seis_errno = SeisbaseCombineErrno(SEIS_ERR_WRUP, errno);
		return NULL;
	}
#endif
	//TODO the data should be able to be updated when it is writing, right?

	StoreType real_store_type;

	if(!ChooseReadStoreType(store_type,real_store_type)) {
		return NULL;
	}

	OpenType open_type = OPEN_WHOLE;

	Updater *updater_ret = (Updater *)OpenUpdaters(real_store_type, open_type, head_filter, row_filter);
	if(NULL == updater_ret) {
		return NULL;
	} else {
		if(!meta_manager_->SetUpdating()) {
			delete updater_ret;
			return NULL;
		}
	}

	return updater_ret;
}

/*******************************************************************************************************
 * use rowfilterbykey
 ******************************************************************************************************/


bool SeisCache::IndexOrderBy(SharedPtr<Order> order) {

}


/*
 * open reader using row filter by key and order.
 */
HeadReader*  SeisCache::OpenHeadReader(SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {
#if 0
	if(meta_manager_->IsWriting()) {
		if(NULL != row_filter) {
			seis_errno = SeisbaseCombineErrno(SEIS_ERR_WRRD, errno);
			return NULL;
		}
	}
#endif
	//TODO if is writing, the data should be able to be read, but could not use order and row_filter_by_key.
}

TraceReader* SeisCache::OpenTraceReader(SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {
#if 0
	if(meta_manager_->IsWriting()) {
		if(NULL != row_filter) {
			seis_errno = SeisbaseCombineErrno(SEIS_ERR_WRRD, errno);
			return NULL;
		}
	}
#endif
	//TODO if is writing, the data should be able to be read, but could not use order and row_filter_by_key.
}

Reader*      SeisCache::OpenReader(SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {
#if 0
	if(meta_manager_->IsWriting()) {
		if(NULL != row_filter) {
			seis_errno = SeisbaseCombineErrno(SEIS_ERR_WRRD, errno);
			return NULL;
		}
	}
#endif
	//TODO if is writing, the data should be able to be read, but could not use order and row_filter_by_key.

}

/*
 * used to accelerate random read. if use HDFS, this will launch the distributed read process. If use NAS, this will launch a Speed read.
 */
HeadSpeedReader *SeisCache::OpenHeadSpeedReader(SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {

}

TraceSpeedReader *SeisCache::OpenTraceSpeedReader(SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {

}

SpeedReader *SeisCache::OpenSpeedReader(SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {

}

/*
 * open updater using rowfilterbykey
 */

HeadUpdater*  SeisCache::OpenHeadUpdater(SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {

}

TraceUpdater* SeisCache::OpenTraceUpdater(SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {

}

Updater* 	  SeisCache::OpenUpdater(SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilterByKey> row_filter,
		SharedPtr<Order> order, StoreType store_type) {

}

/**
 * operator
 */
bool SeisCache::Remove() {

	ScopedPointer<file::SeisFile> seis_file_stable(file::SeisFile::New(data_name_, STABLE));
	ASSERT_NEW(seis_file_stable.data(), false);
	if(!seis_file_stable->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return false;
	}

	if(!seis_file_stable->Remove()) {
		sc_errno = SCCombineErrno(SC_ERR_RM_FILE, errno);
		return false;
	}

	ScopedPointer<file::SeisFile> seis_file_cache(file::SeisFile::New(data_name_, CACHE));
	ASSERT_NEW(seis_file_cache.data(), false);
	if(!seis_file_cache->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return false;
	}
	if(!seis_file_cache->Remove()) {
		sc_errno = SCCombineErrno(SC_ERR_RM_FILE, errno);
		return false;
	}

#if 0
	ScopedPointer<KVInfo> kvinfo_stable(this->OpenKVInfo(STABLE));
	if(NULL == kvinfo_stable) {
		return false;
	}
	if(!kvinfo_stable->Remove()) {
		return false;
	}

	ScopedPointer<KVInfo> kvinfo_cache(this->OpenKVInfo(CACHE));
	if(NULL == kvinfo_cache) {
		return false;
	}
	if(!kvinfo_cache->Remove()) {
		return false;
	}
#endif

	/*
	 * Remove all Index  TODO
	 */
	if(!meta_manager_->Remove()) {
		return false;
	}
	return true;

}


//在rename的过程中，stable和cache只要有一个rename成功了，我们就认为他成功了。只有两个都不成功，我们才认为它不成功。
//例如，如果stable的rename成功了，但是cache的rename失败了，我们就把cache给置为无效，然后继续rename的过程。

bool SeisCache::Rename(const std::string& new_name) {
	int writing_num, updating_num;
	if(!meta_manager_->GetWritingNum(writing_num)) {
		return false;
	}
	if(!meta_manager_->GetUpdatingNum(updating_num)) {
		return false;
	}
	if(writing_num > 0 || updating_num > 0) {
		sc_errno = SC_ERR_BUSY;
		return false;
	}

	ScopedPointer<file::SeisFile> seis_file_stable(file::SeisFile::New(data_name_, STABLE));
	ASSERT_NEW(seis_file_stable.data(), false);
	if(!seis_file_stable->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return false;
	}

	bool rename_stable;

	if(meta_manager_->IsValid(STABLE)) {
		if(!seis_file_stable->Rename(new_name)) {
			if(seis_file_stable->Exists()) { // this means the data is still valid (TODO)
				sc_errno = SCCombineErrno(SC_ERR_RNAM_FILE, errno);
				return false;
			} else { // this means the stable data is no longer valid, remove it
//				this->Remove();
				meta_manager_->SetLifetimeStat(STABLE, INVALID);
				sc_errno = SCCombineErrno(SC_ERR_RNAM_FILE, errno);
				return false;
			}
		}
		rename_stable = true;
	} else {
		meta_manager_->SetLifetimeStat(STABLE, INVALID);
		rename_stable = false;
//		seis_file_stable->Remove();
	}


	ScopedPointer<file::SeisFile> seis_file_cache(file::SeisFile::New(data_name_, CACHE));
	ASSERT_NEW(seis_file_cache.data(), false);
	if(!seis_file_cache->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return false;
	}
	if(meta_manager_->IsValid(CACHE)) {
		if(!seis_file_cache->Rename(new_name)) {

			if(rename_stable) {
				meta_manager_->SetLifetimeStat(CACHE, INVALID);
			} else {
				if(!seis_file_cache->Exists()) {
					meta_manager_->SetLifetimeStat(CACHE, INVALID);
				}
				sc_errno = SCCombineErrno(SC_ERR_RNAM_FILE, errno);
				return false;
			}
		}
	} else {
//		seis_file_cache->Remove();
		meta_manager_->SetLifetimeStat(CACHE, INVALID);
	}
	/*
	 * Rename all Index TODO
	 */
	if(!meta_manager_->CopyMeta(new_name)) {
		ScopedPointer<SeisCache> seis_cache(new SeisCache(new_name));
		seis_cache->Remove();
		sc_errno = SC_ERR_CPY_META;
		return false;
	}

	meta_manager_->SetLifetimeStat(STABLE, INVALID);
	meta_manager_->SetLifetimeStat(CACHE, INVALID);

	return true;
}

bool SeisCache::Copy(const std::string& new_name) {

	/*
	 * Firstly copy data, then copy kvinfo, index, and (meta???)
	 */

	ScopedPointer<SeisCache> seiscache(new SeisCache(new_name));
	ASSERT_NEW(seiscache.data(), false);

	ScopedPointer<Reader> reader(this->OpenReader(NULL, NULL, AUTO));
	if(NULL == reader) {
		return false;
	}

	int head_size = this->GetHeadSize();
	int trace_size = this->GetTraceSize();
	int64_t trace_num = this->GetTraceNum();

	ScopedPointer<char> head_buf(new char[head_size]);
	ScopedPointer<char> trace_buf(new char[trace_size]);

	SharedPtr<HeadType> head_type = new HeadType();
	ASSERT_NEW(head_type, false);

	if(!meta_manager_->GetHeadType(head_type)) {
		return false;
	}

	SharedPtr<TraceType> trace_type = new TraceType();
	ASSERT_NEW(trace_type, false);
	if(!meta_manager_->GetTraceType(trace_type)) {
		return false;
	}

	if(!seiscache->Remove()) {
		return false;
	}

	ScopedPointer<Writer> writer(seiscache->OpenWriter(head_type, trace_type, CACHE));
	if(NULL == writer) {
		return false;
	}

	for(int64_t i = 0; i < trace_num; ++i) {
		if(!reader->Next(head_buf.data(), trace_buf.data())) {
			return false;
		}
		if(!writer->Write(head_buf.data(), trace_buf.data())) {
			return false;
		}
	}
	if(!reader->Close()) {
		return false;
	}
	if(!writer->Close()) {
		return false;
	}

	ScopedPointer<KVInfo> kv_info_src(this->OpenKVInfo());
	if(NULL == kv_info_src) {
		return false;
	}
	ScopedPointer<KVInfo> kv_info_dest(seiscache->OpenKVInfo());
	if(NULL == kv_info_dest) {
		return false;
	}

	std::vector<std::string> key_arr;
	if(!kv_info_src->GetKeys(key_arr)) {
		return false;
	}

	for(std::vector<std::string>::iterator it = key_arr.begin(); it != key_arr.end(); ++it) {
		char *data_buf;
		uint32_t size;
		if(!kv_info_src->Get(*it, data_buf, size)) {
			return false;
		}
		if(!kv_info_dest->Put(*it, data_buf, size)) {
			return false;
		}
	}

	/*
	 * Copy all Index TODO
	 */

	return true;

}

/**
 * property
 */
bool SeisCache::Exists() {

	//if either one is valid, then the whole data is valid

	ScopedPointer<file::SeisFile> seis_file_cache(file::SeisFile::New(data_name_, CACHE));
	ASSERT_NEW(seis_file_cache, -1);

	if(seis_file_cache->Exists()) {
		if(meta_manager_->IsValid(CACHE)) {
			return true;
		}
	}

	ScopedPointer<file::SeisFile> seis_file_stable(file::SeisFile::New(data_name_, STABLE));
	ASSERT_NEW(seis_file_stable, -1);

	if(seis_file_stable->Exists()) {
		if(meta_manager_->IsValid(STABLE)) {
			return true;
		}
	}

	return false;

}

int64_t SeisCache::GetTraceNum() {

	bool is_stable_valid = meta_manager_->IsValid(STABLE);
	bool is_cache_valid = meta_manager_->IsValid(CACHE);

	ScopedPointer<file::SeisFile> seis_file_stable(file::SeisFile::New(data_name_, STABLE));
	ASSERT_NEW(seis_file_stable, -1);

	if(!seis_file_stable->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return -1;
	}
	ScopedPointer<file::SeisFile> seis_file_cache(file::SeisFile::New(data_name_, CACHE));
	ASSERT_NEW(seis_file_cache, -1);

	if(!seis_file_cache->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return -1;
	}

	if(is_stable_valid && is_cache_valid) {
		/*
		 * if both stable and cache valid, check integrity
		 */

		int64_t trace_num_stable = seis_file_stable->GetTraceNum();
		int64_t trace_num_cache = seis_file_cache->GetTraceNum();

		if(trace_num_stable != trace_num_cache) {
			sc_errno = SC_ERR_INTEGRITY;
			return -1;
		} else {
			return trace_num_stable;
		}
	} else {
		/*
		 * return trace num of any data that is valid
		 */
		if(is_cache_valid) {
			return seis_file_cache->GetTraceNum();

		} else if(is_stable_valid) {
			return seis_file_stable->GetTraceNum();
		} else {
			sc_errno = SC_ERR_NOVAL;
			return -1;
		}
	}
}
int64_t SeisCache::Size() {
	int64_t trace_num = GetTraceNum();
	if(trace_num < 0)
		return -1;
	int head_size = GetHeadSize();
	if(head_size < 0)
		return -1;
	int trace_size = GetTraceSize();
	if(trace_size < 0)
		return -1;

	return trace_num * (head_size + trace_size);

} //total size, including trace and head
int SeisCache::GetKeyNum() {
	bool is_stable_valid = meta_manager_->IsValid(STABLE);
	bool is_cache_valid = meta_manager_->IsValid(CACHE);
	if(is_cache_valid) {

		ScopedPointer<file::SeisFile> seis_file_cache(file::SeisFile::New(data_name_, CACHE));
		ASSERT_NEW(seis_file_cache, -1);
		if(!seis_file_cache->Init()) {
			sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
			return -1;
		}

		return seis_file_cache->GetKeyNum();

	} else if(is_stable_valid) {
		ScopedPointer<file::SeisFile> seis_file_stable(file::SeisFile::New(data_name_, STABLE));
		ASSERT_NEW(seis_file_stable, -1);
		if(!seis_file_stable->Init()) {
			sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
			return -1;
		}
		return seis_file_stable->GetKeyNum();
	} else {
//			seis_errno = SeisbaseCombineErrno(SC_ERR_NOVAL, errno);
		return -1;
	}
} // key number in head
int SeisCache::GetTraceSize() {
	bool is_stable_valid = meta_manager_->IsValid(STABLE);
	bool is_cache_valid = meta_manager_->IsValid(CACHE);
	if(is_cache_valid) {

		ScopedPointer<file::SeisFile> seis_file_cache(file::SeisFile::New(data_name_, CACHE));
		ASSERT_NEW(seis_file_cache, -1);

		if(!seis_file_cache->Init()) {
			sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
			return -1;
		}

		return seis_file_cache->GetTraceSize();

	} else if(is_stable_valid) {

		ScopedPointer<file::SeisFile> seis_file_stable(file::SeisFile::New(data_name_, STABLE));
		ASSERT_NEW(seis_file_stable, -1);

		if(!seis_file_stable->Init()) {
			sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
			return -1;
		}
		return seis_file_stable->GetTraceSize();
	} else {

		sc_errno = SC_ERR_NOVAL;
		return -1;
	}
} //get the length of a trace, in bytes
int SeisCache::GetHeadSize() {
	bool is_stable_valid = meta_manager_->IsValid(STABLE);
	bool is_cache_valid = meta_manager_->IsValid(CACHE);
	if(is_cache_valid) {

		ScopedPointer<file::SeisFile> seis_file_cache(file::SeisFile::New(data_name_, CACHE));
		ASSERT_NEW(seis_file_cache, -1);

		if(!seis_file_cache->Init()) {
			sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
			return -1;
		}

		return seis_file_cache->GetHeadSize();

	} else if(is_stable_valid) {
		ScopedPointer<file::SeisFile> seis_file_stable(file::SeisFile::New(data_name_, STABLE));
		ASSERT_NEW(seis_file_stable, -1);

		if(!seis_file_stable->Init()) {
			sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
			return -1;
		}
		return seis_file_stable->GetHeadSize();
	} else {
		sc_errno = SC_ERR_NOVAL;
		return -1;
	}
} // get the length of a head, in bytes

time_t SeisCache::LastModified() {
	return meta_manager_->GetLastModified();
}
time_t SeisCache::LastAccessed() {
	return meta_manager_->GetLastAccessed();
}

std::string SeisCache::GetName() {
	return data_name_;
}

DataState SeisCache::GetState() {

}

/*
 * merge the data in merge_list into new_name. Users have to provide a KVInfoMerger instance to merge extern information
 * in their own way
 */
bool SeisCache::Merge(const std::string &new_name, const std::vector<std::string> &merge_list, KVInfoMerger *merger) {
	if(merge_list.empty()) {
		return true;
	}

	StoreType merge_type = AUTO;
	bool valid_merge = false;
	int num_stable = 0, num_cache = 0;

	for(std::vector<std::string>::const_iterator it = merge_list.begin(); it != merge_list.end(); ++it) {
		std::string merge_name = *it;
		ScopedPointer<MetaManager> p_meta(new MetaManager(merge_name));
		if(p_meta->IsValid(STABLE)) {
			num_stable ++;
		}
		if(p_meta->IsValid(CACHE)) {
			num_cache ++;
		}
	}

	if(num_stable == merge_list.size()) {
		merge_type = STABLE;
		valid_merge = true;
	}
	if(num_cache == merge_list.size()) {
		merge_type = CACHE;
		valid_merge = true;
	}
	if(!valid_merge) {
		sc_errno = SC_ERR_MG_DIF_TYPE;
		return false;
	}

	ScopedPointer<file::SeisFile> seis_file_new(file::SeisFile::New(new_name, merge_type));
	ASSERT_NEW(seis_file_new.data(), false);

	if(!seis_file_new->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return false;
	}

	ScopedPointer<SeisCache> seis_cache_new(new SeisCache(new_name));
	ASSERT_NEW(seis_cache_new.data(), false);

	ScopedPointer<KVInfo> kv_info_new(seis_cache_new->OpenKVInfo(merge_type));
	if(NULL == kv_info_new.data()) {
		return false;
	}

	ScopedPointer<MetaManager> meta_new(new MetaManager(new_name));
	ASSERT_NEW(meta_new.data(), false);

	if(!meta_new->InitAllMeta()) {
		return false;
	}

	for(std::vector<std::string>::const_iterator it = merge_list.begin(); it != merge_list.end(); ++it) {
		std::string merge_name = *it;
		if(!seis_file_new->Merge(merge_name)) {
			sc_errno = SCCombineErrno(SC_ERR_MERGE_FILE, errno);
			return false;
		}

		ScopedPointer<SeisCache> seis_cache_merge(new SeisCache(merge_name));
		ScopedPointer<KVInfo> kv_info_merge(seis_cache_merge->OpenKVInfo(merge_type));
		if(NULL == kv_info_merge.data()) {
			return false;
		}
		if(!merger->Merge(kv_info_new.data(), kv_info_merge.data())) {
			sc_errno = SCCombineErrno(SC_ERR_MERGE_FKV, errno);
			return false;
		}

		if(it == merge_list.begin()) {
			HeadType head_type;
			TraceType trace_type;
			ScopedPointer<MetaManager> meta_merge(new MetaManager(merge_name));
			ASSERT_NEW(meta_merge.data(), false);
			if(!meta_merge->GetHeadType(&head_type)) {
				return false;
			}
			if(!meta_merge->GetTraceType(&trace_type)) {
				return false;
			}
			if(!meta_new->SaveHeadType(head_type)) {
				return false;
			}
			if(!meta_new->SaveTraceType(trace_type)) {
				return false;
			}
		}
	}

	//TODO remove all index

	return true;


}

}

}

