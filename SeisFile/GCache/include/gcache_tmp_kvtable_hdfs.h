/*
 * gcache_tmp_kvtable_hdfs.h
 *
 *  Created on: Jan 20, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_KVTABLE_HDFS_H_
#define GCACHE_TMP_KVTABLE_HDFS_H_

#include <stdint.h>
#include <list>
#include <string>
#include <map>

#include "hdfs.h"
#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_kvtable.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

struct MetaTMPHDFS;
class Storage;

#define OBJ_DIR_NUM                100

/**
 * KVTableHDFS class is a child of KVTable.
 * this class provides some functions of key-value operations in HDFS.
 */
class KVTableHDFS: public KVTable {
public:

    /**
     * destructor, destruct a KVTableHDFS object.
     */
    virtual ~KVTableHDFS();

    /**
     * open a table if the table exists, if the table doesn't exist then create a table then open this new table.
     */
    virtual bool Open(lifetime_t lifetime_days = LIFETIME_DEFAULT, CompressAdvice cmp_adv = CMP_NON,
            CacheLevel cache_lv = CACHE_L3);

    /**
     * put a key value pair into this table.
     */
    virtual bool Put(const std::string &key, const char* data, int64_t size);

    /**
     * delete a key-value pair of table.
     */
    virtual bool Delete(const std::string &key);

    /**
     * get the corresponding data and the size of this data according to the key.
     */
    virtual bool Get(const std::string &key, char*& data, int64_t& size);

    /**
     * get the size of corresponding data according to the key, in bytes.
     */
    virtual int64_t SizeOf(const std::string &key);

    /**
     * get the key set of this table.
     */
    virtual std::list<std::string> GetKeys();

    /**
     * get the size of this table, in bytes.
     */
    virtual int64_t Size();

    /**
     * determine whether this table exists.
     */
    virtual bool Exists();

    /**
     * get the life time of this table, in days.
     */
    virtual lifetime_t Lifetime() const;

    /**
     * set the life time of this table, in days.
     */
    virtual bool SetLifetime(lifetime_t lifetime_days);

    /**
     * remove this table exists.
     */
    virtual bool Remove();

    /**
     * rename the table old name to new name.
     */
    virtual bool Rename(const std::string &new_name);

    /**
     * get the name of this table.
     */
    virtual std::string GetName() const;

    /**
     * determine whether this table is expired or not.
     */
    virtual bool LifetimeExpired() const;

    friend class KVTable;

private:

    /**
     * constructor, construct a KVTableHDFS object.
     */
    KVTableHDFS(const std::string &table_name);

    /**
     * initialize  member variable.
     */
    virtual bool Init();

    /**
     * get the meta data of this table.
     */
    MetaTMPHDFS *GetMeta() const;

    /**
     * get the full file name of this table, full file name contains the root directory.
     */
    std::string GetFullTableName() const;

    /**
     * get the file name of object map into this key.
     */
    std::string GetObjFilename(const std::string &key);

    /**
     * get the root directory of this file.
     */
    std::string GetRootDirectory() const;

    /**
     * get the hdfsFS by root directory.
     */
    hdfsFS GetRootFS(std::string rootDir) const;

    void TouchTable(hdfsFS fs);

    /*member variable*/
    std::string _table_name;                //the name of table.
    Storage* _storage;                      //member storage, used for meta data operation.
    std::map<std::string, hdfsFS> *_rootFS;  //contains rootDir and hdfsFS key-value pairs.
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_KVTABLE_HDFS_H_ */
