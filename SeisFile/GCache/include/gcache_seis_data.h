/*
 * gcache_seis_data.h
 *
 *  Created on: Dec 14, 2015
 *      Author: wyd
 */

#ifndef GCACHE_SEIS_DATA_H_
#define GCACHE_SEIS_DATA_H_

#include <sys/types.h>
#include <map>

#include <hdfs.h>
#include <gcache_seis_kvinfo.h>
#include <vector>

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "gcache_seis_storage.h"

#include "seisfile_headupdater.h"
#include "seisfile_headwriter.h"
#include "seisfile_headreader.h"
#include "gcache_seis_tracewriter.h"
#include "gcache_seis_tracereader.h"
#include "gcache_seis_traceupdater.h"

#include "gcache_seis_reader.h"
#include "gcache_seis_writer.h"
#include "seisfile.h"
#include <uuid/uuid.h>

NAMESPACE_BEGIN_SEISFS
namespace file {

class HDFS: public SeisFile {
public:

    /**
     * Constructor.
     */
    HDFS(const std::string& data_name);

    static SeisFile* New(const std::string& data_name);
//    static bool Merge(const std::string &strNewName,const std::vector<std::string>& src_data_names);

    /**
     * Destructor.
     */
    ~HDFS();

    virtual bool Init();

    virtual std::string GetClassName() {
    	return "HDFS";
    }

    /**
     * Update the meta.
     */
    bool SetSortKeys(const SortKeys& sort_keys); //?

    /**
     * Get the sort keys.
     */
    SortKeys GetSortKeys(); //?

    /**
     * Like k-v table. Reel filename format: {data_name}.{key}.reel
     */
    KVInfo* OpenKVInfo(); //?

    /*
     * Read only
     */
    HeadReader* OpenHeadReader();
    TraceReader* OpenTraceReader();
    Reader* OpenReader();

    /*
     * Append only
     */
    HeadWriter* OpenHeadWriter(const HeadType& type, Lifetime lifetime_days =
    LIFETIME_DEFAULT);
    TraceWriter* OpenTraceWriter(const TraceType& traceType, Lifetime lifetime_days =
    LIFETIME_DEFAULT);
    Writer* OpenWriter(const HeadType& headType, const TraceType& traceType,
            Lifetime lifetime_days = LIFETIME_DEFAULT);
    HeadWriter* OpenHeadWriter();
    TraceWriter* OpenTraceWriter();
    Writer* OpenWriter();

    /*
     * update only
     */
    HeadUpdater* OpenHeadUpdater();
    TraceUpdater* OpenTraceUpdater();

    bool Remove();

    bool Rename(const std::string& new_name);

    bool Copy(const std::string& newName);

    bool SetLifetime(Lifetime lifetime_days);

    int GetKeyNum();

    /**
     * property
     */
    bool Exists();

    int64_t Size(); //include head

    int GetTraceSize();

    int GetHeadSize();

    Lifetime GetLifetime();

    time_t LastModified();

    time_t LastAccessed();

    std::string GetName();

    LifetimeStat GetLifetimeStat(); //?

    //set meta tag
    void SetLifetimeStat(LifetimeStat tag);

    //ice-----------
    /**
     * operator
     */
    bool Merge(const std::string& src_data_name);

    bool Compact();
    int64_t GetUpdatedTraceRows();
    int64_t GetUpdatedHeadRows();
    //--------------

    //not recommend to use
    bool TruncateTrace(int64_t traces);
    bool TruncateHead(int64_t heads);

    //get trace number , returns the small number of heads or traces,
    uint32_t GetTraceNum();


private:

    /**
     * Init members.
     */

    bool RecursiveCopy(const std::string &real_src_path, const std::string &real_dest_path);

    StorageSeis* _storage_seis;
    std::string _data_name;
    std::string _hdfs_data_name;
    std::string _hdfs_meta_name;
    std::string _root_dir;
    SharedPtr<MetaSeisHDFS> _meta;

    std::string _cur_hdfs_data_name;
    std::vector<std::string> _seis_data_list;
    hdfsFS _fs;
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_DATA_H_ */
