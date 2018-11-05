/*
 * gcache_tmp_kvtable_local.h
 *
 *  Created on: Jan 20, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_KVTABLE_LOCAL_H_
#define GCACHE_TMP_KVTABLE_LOCAL_H_

#include <sys/types.h>
#include <list>
#include <string>
#include <bits/functional_hash.h>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_kvtable.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

struct MetaTMPLocal;
class Storage;

#define OBJ_DIR_NUM                100

class KVTableLocal: public KVTable {
public:
    virtual ~KVTableLocal();

    virtual bool Open(lifetime_t lifetime_days = LIFETIME_DEFAULT, CompressAdvice cmp_adv = CMP_NON,
            CacheLevel cache_lv = CACHE_L3);

    virtual bool Put(const std::string &key, const char* data, int64_t size);

    virtual bool Delete(const std::string &key);

    virtual bool Get(const std::string &key, char*& data, int64_t& size);

    virtual int64_t SizeOf(const std::string &key);

    virtual std::list<std::string> GetKeys();

    virtual int64_t Size();

    virtual bool Exists();

    virtual lifetime_t Lifetime() const;

    virtual bool SetLifetime(lifetime_t lifetime_days);

    virtual bool Remove();

    virtual bool Rename(const std::string &new_name);

    virtual std::string GetName() const;

    virtual bool LifetimeExpired() const;

    friend class KVTable;

private:

    KVTableLocal(const std::string &table_name);

    virtual bool Init();

    MetaTMPLocal *GetMeta() const;

    std::string GetFullTableName() const;

    std::string GetObjFilename(const std::string &key);

    void TouchTable();

    std::string _table_name;
    Storage* _storage;
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_TMP_KVTABLE_LOCAL_H_ */
