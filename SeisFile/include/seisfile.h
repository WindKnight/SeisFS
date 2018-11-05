/*
 * seisfile.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_H_
#define SEISFILE_H_

#include <string>
#include <map>
#include "seisfile_meta.h"
#include "seisfs_config.h"


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

#define LIFETIME_DEFAULT 30

class SeisFile {
public:

	static SeisFile *New(const std::string &data_name, StoreType store_type);

	SeisFile();

	SeisFile(const std::string &data_name);

    virtual ~SeisFile();

    virtual std::string GetClassName() = 0;

    virtual bool Init() = 0;
    /**
     * Record the sort order of this data instance
     */
    virtual bool SetSortKeys(const SortKeys& sort_keys) = 0;

    /**
     * Get the sort keys.
     */
    virtual SortKeys GetSortKeys() = 0;

    /**
     * Like k-v table. SFReel filename format: {data_name}.{key}.SFReel
     */
    virtual KVInfo* OpenKVInfo() = 0;

    /*
     * Read only
     */
    virtual HeadReader* OpenHeadReader() = 0;
    virtual TraceReader* OpenTraceReader() = 0;
    virtual Reader* OpenReader() = 0;

    /*
     * Append only
     */
    virtual HeadWriter* OpenHeadWriter(const HeadType &head_type, Lifetime lifetime_days = LIFETIME_DEFAULT) = 0;
    virtual TraceWriter* OpenTraceWriter(const TraceType& trace_type, Lifetime lifetime_days = LIFETIME_DEFAULT) = 0;
    virtual Writer* OpenWriter(const HeadType &head_type, const TraceType& trace_type, Lifetime lifetime_days = LIFETIME_DEFAULT) = 0;

    virtual HeadWriter* OpenHeadWriter() = 0;
    virtual TraceWriter* OpenTraceWriter() = 0;
    virtual Writer* OpenWriter() = 0;

    /*
     * update only
     */
    virtual HeadUpdater* OpenHeadUpdater() = 0;
    virtual TraceUpdater* OpenTraceUpdater() = 0;

    virtual bool Remove() = 0;

    virtual bool Rename(const std::string& new_name) = 0;

    virtual bool Copy(const std::string& newName) = 0;

    virtual bool SetLifetime(Lifetime lifetime_days) = 0;
    /*
     * merge src_data_name to current data.
     *
     */
    virtual bool Merge(const std::string& src_data_name) = 0;

    /*
     * truncate
     */
    virtual bool TruncateTrace(int64_t traces) = 0;
    virtual bool TruncateHead(int64_t heads) = 0;

    /**
     * property
     */
    virtual bool Exists() = 0;

    virtual int64_t Size() = 0; //total size in bytes, include head

    virtual int GetKeyNum() = 0;

    virtual int GetTraceSize() = 0;//return length of one trace, in bytes

    virtual int GetHeadSize() = 0;//return length of one head, in bytes

    //get trace number , returns the small number of heads or traces,
    virtual uint32_t GetTraceNum() = 0;

    virtual Lifetime GetLifetime() = 0;

    virtual time_t LastModified() = 0;

    virtual time_t LastAccessed() = 0;

    virtual std::string GetName() = 0;

    virtual LifetimeStat GetLifetimeStat() = 0;

    virtual void SetLifetimeStat(LifetimeStat lt_stat) = 0;

protected:
    std::string data_name_;

private:

};


template <typename T>
SeisFile *CreateObj(const std::string &data_name) {
	return (new T(data_name));
}

typedef SeisFile* (*SeisFileConstructor)(const std::string &data_name);


struct TypeMapping {
	std::string type_name;
	SeisFileConstructor constr;
	std::pair<std::string, SeisFileConstructor> MakePare() const {
		return std::make_pair(type_name, constr);
	}
};

class SeisFileReflectionManager {
public:

	static void SetClasses(const TypeMapping mapping[], int map_num);
	static SeisFile *GetInstance(const std::string &type_name, const std::string &data_name);

private:
	static bool is_init_;
	static std::map<std::string, SeisFileConstructor> constructor_map_;
};




}

}


#endif /* SEISFILE_H_ */
