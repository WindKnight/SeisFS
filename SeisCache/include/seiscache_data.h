/*
 * seiscache_data.h
 *
 *  Created on: Mar 22, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_DATA_H_
#define SEISCACHE_DATA_H_

#include "seisfs_config.h"
#include "util/seisfs_shared_pointer.h"
#include "seiscache_datatype.h"
#include <vector>

namespace seisfs {

class HeadFilter;
class RowFilter;

namespace cache {

class KVInfo;
class HeadReader;
class TraceReader;
class Reader;
class ReaderBase;

class HeadSpeedReader;
class TraceSpeedReader;
class SpeedReader;

class HeadWriter;
class TraceWriter;
class Writer;
class WriterBase;

class HeadUpdater;
class TraceUpdater;
class Updater;
class UpdaterBase;

class KVInfoMerger;

class RowFilterByKey;
class Order;

class MetaManager;


enum OpenType {
	OPEN_HEAD = 0,
	OPEN_TRACE = 1,
	OPEN_WHOLE = 2
};


class SeisCache {
public:

	SeisCache(const std::string& data_name);

	~SeisCache();

	SeisCache(const SeisCache& data);

	SeisCache& operator= (const SeisCache& other);

	/**
	 * ExtInfo of data. Key-value store.
	 */
	KVInfo* OpenKVInfo(StoreType store_type = AUTO);

	/*
	 * Read only.
	 * use head_filter to choose keys that you want to read
	 * use row_filter to choose traces that you want to read
	 * use storage_type to choose the seis data source, for example ,from nas, hdfs. default is AUTO, which means seiscache will choose
	 * the appropriate seis data AUTOally.
	 */
	HeadReader*  OpenHeadReader(SharedPtr<HeadFilter> head_filter,
			SharedPtr<RowFilter> row_filter, StoreType store_type = AUTO);

	TraceReader* OpenTraceReader(SharedPtr<RowFilter> row_filter,
			StoreType store_type = AUTO);

	Reader*      OpenReader(SharedPtr<HeadFilter> head_filter,
			SharedPtr<RowFilter> row_filter, StoreType store_type = AUTO);

	/*
	 * used to accelerate random read. if use HDFS, this will launch the distributed read process. If use NAS, this will launch a Speed read.
	 */
	HeadSpeedReader *OpenHeadSpeedReader(SharedPtr<HeadFilter> head_filter,
			SharedPtr<RowFilter> row_filter, StoreType store_type = AUTO);

	TraceSpeedReader *OpenTraceSpeedReader(SharedPtr<RowFilter> row_filter,
			StoreType store_type = AUTO);

	SpeedReader *OpenSpeedReader(SharedPtr<HeadFilter> head_filter,
			SharedPtr<RowFilter> row_filter, StoreType store_type = AUTO);

	/*
	 * Append only
	 * head type is used to set the key number and size of each key
	 * trace type is used to set length of one trace.
	 */
	HeadWriter*  OpenHeadWriter(SharedPtr<HeadType> head_type = NULL, StoreType store_type = AUTO);

	TraceWriter* OpenTraceWriter(SharedPtr<TraceType> trace_type = NULL, StoreType store_type = AUTO);

	Writer*      OpenWriter(SharedPtr<HeadType> head_type = NULL, SharedPtr<TraceType> trace_type = NULL, StoreType store_type = AUTO);


	/*
	 * update only
	 */
	HeadUpdater*  OpenHeadUpdater(SharedPtr<HeadFilter> head_filter = NULL,
			SharedPtr<RowFilter> row_filter = NULL, StoreType store_type = AUTO);

	TraceUpdater* OpenTraceUpdater(SharedPtr<RowFilter> row_filter = NULL,
			StoreType store_type = AUTO);

	Updater* 	  OpenUpdater(SharedPtr<HeadFilter> head_filter = NULL,
			SharedPtr<RowFilter> row_filter = NULL, StoreType store_type = AUTO);




	/*******************************************************************************************************
	 * use rowfilterbykey
	 ******************************************************************************************************/

	/*
	 * return if the index exists
	 */
	bool IndexOrderBy(SharedPtr<Order> order);
	/*
	 * build index
	 */
//	GPP::Future<bool> CreateIndex(SharedPtr<Order> order);

	/*
	 * open reader using row filter by key and order.
	 */
	HeadReader*  OpenHeadReader(SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilterByKey> row_filter,
			SharedPtr<Order> order, StoreType store_type = AUTO);

	TraceReader* OpenTraceReader(SharedPtr<RowFilterByKey> row_filter,
			SharedPtr<Order> order, StoreType store_type = AUTO);

	Reader*      OpenReader(SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilterByKey> row_filter,
			SharedPtr<Order> order, StoreType store_type = AUTO);

	/*
	 * used to accelerate random read. if use HDFS, this will launch the distributed read process. If use NAS, this will launch a Speed read.
	 */
	HeadSpeedReader *OpenHeadSpeedReader(SharedPtr<HeadFilter> head_filter = NULL, SharedPtr<RowFilterByKey> row_filter = NULL,
			SharedPtr<Order> order = NULL, StoreType store_type = AUTO);

	TraceSpeedReader *OpenTraceSpeedReader(SharedPtr<RowFilterByKey> row_filter = NULL,
			SharedPtr<Order> order = NULL, StoreType store_type = AUTO);

	SpeedReader *OpenSpeedReader(SharedPtr<HeadFilter> head_filter = NULL, SharedPtr<RowFilterByKey> row_filter = NULL,
			SharedPtr<Order> order = NULL, StoreType store_type = AUTO);

	/*
	 * open updater using rowfilterbykey
	 */
	HeadUpdater*  OpenHeadUpdater(SharedPtr<HeadFilter> head_filter = NULL, SharedPtr<RowFilterByKey> row_filter = NULL,
			SharedPtr<Order> order = NULL, StoreType store_type = AUTO);

	TraceUpdater* OpenTraceUpdater(SharedPtr<RowFilterByKey> row_filter = NULL,
			SharedPtr<Order> order = NULL, StoreType store_type = AUTO);

	Updater* 	  OpenUpdater(SharedPtr<HeadFilter> head_filter = NULL, SharedPtr<RowFilterByKey> row_filter = NULL,
			SharedPtr<Order> order = NULL, StoreType store_type = AUTO);

	/**
	 * operator
	 */
	bool Remove();

	bool Rename(const std::string& new_name);

	bool Copy(const std::string& new_name);

	/**
	 * property
	 */
	bool Exists();

	int64_t GetTraceNum();

	int64_t GetGatherNum(const Order &order);

	int64_t Size(); //total size, including trace and head

	int GetKeyNum(); // key number in head

	int GetTraceSize(); //get the length of a trace, in bytes
	int GetHeadSize(); // get the length of a head, in bytes

	time_t LastModified();
	time_t LastAccessed();

	std::string GetName();

	DataState GetState();

	/*
	 * merge the data in merge_list into new_name. Users have to provide a SCExtInfoMerger instance to merge extern information
	 * in their own way
	 */
	static bool Merge(const std::string &new_name, const std::vector<std::string> &merge_list, KVInfoMerger *merger);

private:

	bool CheckDataIntegrity();

	bool ChooseReadStoreType(StoreType store_type, StoreType &real_store_type);
	ReaderBase *OpenReaders(StoreType real_store_type, OpenType open_type,
			SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilter> row_filter);

	bool ChooseWriterStoreType(StoreType store_type, StoreType &real_store_type, bool &open_new);
	WriterBase *OpenWriters(StoreType store_type, OpenType open_type, bool open_new,
			SharedPtr<HeadType> head_type, SharedPtr<TraceType> trace_type);

//	bool ChooseUpdaterStoreType(StoreType store_type, StoreType &real_store_type);
	//choose the same store type with read
	UpdaterBase *OpenUpdaters(StoreType real_store_type, OpenType open_type,
			SharedPtr<HeadFilter> head_filter, SharedPtr<RowFilter> row_filter);

	std::string data_name_;
	SharedPtr<MetaManager> meta_manager_;
};

}

}


#endif /* SEISCACHE_DATA_H_ */
