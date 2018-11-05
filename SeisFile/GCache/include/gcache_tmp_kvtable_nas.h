/*
 * gcache_tmp_kvtable_nas.h
 *
 *  Created on: Jan 20, 2016
 *      Author: wb
 */

#ifndef GCACHE_TMP_KVTABLE_NAS_H_
#define GCACHE_TMP_KVTABLE_NAS_H_

#include <sys/types.h>
#include <list>
#include <string>
#include <bits/functional_hash.h>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_kvtable.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

struct MetaTMPNAS;
class Storage;

#define OBJ_DIR_NUM                100

/**
 * KVTableNAS class is a child of KVTable.
 * this class provides some functions of key-value operations in NAS.
 */
class KVTableNAS: public KVTable {
public:

    /**
     * Destructor, destruct a KVTableNAS object.
     */
    virtual ~KVTableNAS();

    /**
     * Open a table if the table exists, if the table doesn't exist then create a table then open this new table.
     */
    virtual bool Open(lifetime_t lifetime_days = LIFETIME_DEFAULT, CompressAdvice cmp_adv = CMP_NON,
            CacheLevel cache_lv = CACHE_L3);

    /**
     * Put a key value pair into this table.
     */
    virtual bool Put(const std::string &key, const char* data, int64_t size);

    /**
     * Delete a key-value pair of table.
     */
    virtual bool Delete(const std::string &key);

    /**
     * Get the corresponding data and the size of this data according to the key.
     */
    virtual bool Get(const std::string &key, char*& data, int64_t& size);

    /**
     * Get the size of corresponding data according to the key, in bytes.
     */
    virtual int64_t SizeOf(const std::string &key);

    /**
     * Get the key set of this table.
     */
    virtual std::list<std::string> GetKeys();

    /**
     * Get the size of this table, in bytes.
     */
    virtual int64_t Size();

    /**
     * Determine whether this table exists.
     */
    virtual bool Exists();

    /**
     * Get the life time of this table, in days.
     */
    virtual lifetime_t Lifetime() const;

    /**
     * Set the life time of this table, in days.
     */
    virtual bool SetLifetime(lifetime_t lifetime_days);

    /**
     * Remove this table exists.
     */
    virtual bool Remove();

    /**
     * Rename the table old name to new name.
     */
    virtual bool Rename(const std::string &new_name);

    /**
     * Get the name of this table.
     */
    virtual std::string GetName() const;

    /**
     * Determine whether this table is expired or not.
     */
    virtual bool LifetimeExpired() const;

    friend class KVTable;

private:

    /**
     * Constructor, construct a KVTableNAS object.
     */
    KVTableNAS(const std::string &table_name);

    /**
     * Initialize  member variable.
     */
    virtual bool Init();

    /**
     * Get the meta data of this table.
     */
    MetaTMPNAS *GetMeta() const;

    /**
     * Get the full file name of this table, full file name contains the root directory.
     */
    std::string GetFullTableName() const;

    /**
     * Get the file name of object map into this key.
     */
    std::string GetObjFilename(const std::string &key);

    /**
     * Set the table access and modification times of the table filename.
     */
    void TouchTable();

    /*member variable*/
    std::string _table_name;                //the name of table.
    Storage* _storage;                      //member storage, used for meta data operation.
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_KVTABLE_NAS_H_ */
