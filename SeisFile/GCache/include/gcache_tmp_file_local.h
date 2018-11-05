/*
 * gcache_tmp_file_local.h
 *
 *  Created on: Dec 29, 2015
 *      Author: wyd
 */

#ifndef GCACHE_TMP_FILE_LOCAL_H_
#define GCACHE_TMP_FILE_LOCAL_H_

#include <sys/types.h>
#include <cstdint>
#include <string>
#include <boost/smart_ptr/shared_ptr.hpp>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_file.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class Reader;
class Writer;
class KVTable;
struct MetaTMPLocal;
class Storage;

class FileLocal: public File {
public:
    virtual ~FileLocal();

    virtual Reader *OpenReader(FAdvice advice = FADV_NONE);

    virtual Writer *OpenWriter(lifetime_t lifetime_days = LIFETIME_DEFAULT, CacheLevel cache_lv =
            CACHE_L3, FAdvice advice = FADV_NONE);

    virtual int64_t Size() const;

    virtual lifetime_t Lifetime() const;

    virtual bool Exists() const;

    virtual bool Remove();

    virtual bool Truncate(int64_t size);

    virtual bool SetLifetime(lifetime_t lifetime_days);

    virtual bool Copy(const std::string &new_name, CacheLevel cache_lv);

    virtual bool Copy(const std::string &newName);

    virtual bool Rename(const std::string &new_name);

    virtual bool Link(const std::string &link_name);

    virtual uint64_t LastModified() const; // return millisecond

    virtual std::string GetName() const;

    virtual bool LifetimeExpired() const;

    virtual void Lock();

    virtual void Unlock();

    friend class File;

private:
    FileLocal(const std::string &filename);

    virtual bool Init();

    MetaTMPLocal *GetMeta() const;

    std::string GetRealFilename() const;

    boost::shared_ptr<bool> _isWriterAlive;        //indicate if this writer is alive
    std::string _filename;
    Storage* _storage;
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_FILE_LOCAL_H_ */
