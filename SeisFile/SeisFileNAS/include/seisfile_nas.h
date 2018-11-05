/*
 * seisfile_nas.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_NAS_H_
#define SEISFILE_NAS_H_

#include <string>
#include <map>
#include "seisfile.h"
#include "seisfile_meta.h"
#include "seisfs_config.h"
#include "seisfs_meta.h"

#include <GEFile.h>

////////////////////
#include "seisfile_nas_writer.h"
#include "seisfile_nas_tracewriter.h"
#include "seisfile_nas_headwriter.h"
#include "seisfile_nas_reader.h"
#include "seisfile_nas_traceupdater.h"
#include "seisfile_nas_headupdater.h"
#include "seisfile_nas_kvinfo.h"
#include "seisfile_nas_headreader.h"
#include "seisfile_nas_tracereader.h"

namespace seisfs {

namespace file {

class KVInfo;
class HeadReader;
class TraceReader;
class Reader;
class HeadWriter;
class TraceWriter;
class Writer;
class HeadUpdater;
class TraceUpdater;

class SeisFileNAS : public SeisFile {
public:

	SeisFileNAS(const std::string &data_name);

    virtual ~SeisFileNAS();

    virtual std::string GetClassName() {
    	return "SeisFileNAS";
    }

    virtual bool Init();
    /**
     * Record the sort order of this data instance
     */
    virtual bool SetSortKeys(const SortKeys& sort_keys);

    /**
     * Get the sort keys.
     */
    virtual SortKeys GetSortKeys();

    /**
     * Like k-v table. SFReel filename format: {data_name}.{key}.SFReel
     */
    virtual KVInfo* OpenKVInfo();

    /*
     * Read only
     */
    virtual HeadReader* OpenHeadReader();
    virtual TraceReader* OpenTraceReader();
    virtual Reader* OpenReader();

    /*
     * Append only
     */
    virtual HeadWriter* OpenHeadWriter(const HeadType &head_type, Lifetime lifetime_days = LIFETIME_DEFAULT);
    virtual TraceWriter* OpenTraceWriter(const TraceType& trace_type, Lifetime lifetime_days = LIFETIME_DEFAULT);
    virtual Writer* OpenWriter(const HeadType &head_type, const TraceType& trace_type, Lifetime lifetime_days = LIFETIME_DEFAULT);

    virtual HeadWriter* OpenHeadWriter();
    virtual TraceWriter* OpenTraceWriter();
    virtual Writer* OpenWriter();

    /*
     * update only
     */
    virtual HeadUpdater* OpenHeadUpdater();
    virtual TraceUpdater* OpenTraceUpdater();

    virtual bool Remove();

    virtual bool Rename(const std::string& new_name);

    virtual bool Copy(const std::string& newName);

    virtual bool SetLifetime(Lifetime lifetime_days);
    /*
     * merge src_data_name to current data.
     * if merge fail, both of the original data should be the same with those before merge.
     */
    virtual bool Merge(const std::string& src_data_name);

    virtual bool MergeHead(const std::string& src_data_name);

    virtual bool MergeTrace(const std::string& src_data_name);

    /*
     * truncate
     */
    virtual bool TruncateTrace(int64_t traces);
    virtual bool TruncateHead(int64_t heads);

    /**
     * property
     */
    virtual bool Exists();

    virtual int64_t Size(); //total size in bytes, include head

    virtual int GetKeyNum();

    virtual int GetTraceSize();//return length of one trace, in bytes

    virtual int GetHeadSize();//return length of one head, in bytes

    //get trace number , returns the small number of heads or traces,
    virtual uint32_t GetTraceNum();

    virtual Lifetime GetLifetime();

    virtual time_t LastModified();

    virtual time_t LastAccessed();

    virtual std::string GetName();

    virtual LifetimeStat GetLifetimeStat();

    virtual void SetLifetimeStat(LifetimeStat lt_stat);


private:

    //bool Rename_part(std::string newname, std::string oldname);

    void ReadMeta();

    std::string _data_name;
    std::string _sortkey_filename;

    std::string seisnas_home_path_;
    std::string _meta_filename;

//   std::string seisnas_home_path_;

    GEFile *_gf_sortkeys;
    std::vector<SeisKey> _sort_keys;

    MetaNAS *_meta;

    WriterNAS *_writer;
    HeadWriterNAS *_headwriter;
    TraceWriterNAS *_tracewriter;
    ReaderNAS *_reader;

    /*HeadType GetHeadType();
    TraceType GetTraceType();*/


};


}

}


#endif /* SEISFILE_NAS_H_ */
