/*
 * gcache_tmp_kvtable.h
 *
 *  Created on: Jan 20, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_KVTABLE_H_
#define GCACHE_TMP_KVTABLE_H_

#include <sys/types.h>
#include <list>
#include <string>

#include "gcache_global.h"
#include "gcache_advice.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class KVTable {
public:

    virtual ~KVTable() {
    }
    ;

    static KVTable* New(const std::string &table_name, StorageArch &s_arch);

    virtual bool Open(lifetime_t lifetime_days = LIFETIME_DEFAULT, CompressAdvice cmp_adv = CMP_NON,
            CacheLevel cache_lv = CACHE_L3) = 0;

    virtual bool Put(const std::string &key, const char* data, int64_t size) = 0;

    virtual bool Delete(const std::string &key) = 0;

    virtual bool Get(const std::string &key, char*& data, int64_t& size) = 0;

    virtual int64_t SizeOf(const std::string &key) = 0;

    virtual std::list<std::string> GetKeys() = 0;

    virtual int64_t Size() = 0;

    virtual bool Exists() = 0;

    virtual lifetime_t Lifetime() const = 0;

    virtual bool SetLifetime(lifetime_t lifetime_days) = 0;

    virtual bool Remove() = 0;

    virtual bool Rename(const std::string &new_name) = 0;

    virtual std::string GetName() const = 0;

    virtual bool LifetimeExpired() const = 0;

protected:
    KVTable() {
    }
    ;

private:
    virtual bool Init() = 0;

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_KVTABLE_H_ */
