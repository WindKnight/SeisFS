/*
 * gcache_tmp_file_nas.h
 *
 *  Created on: Dec 29, 2015
 *      Author: wb
 */

#ifndef GCACHE_TMP_FILE_CENTRAL_H_
#define GCACHE_TMP_FILE_CENTRAL_H_

#include <sys/types.h>
#include <cstdint>
#include <string>
#include <boost/smart_ptr/shared_ptr.hpp>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_file.h"
#include "gcache_tmp_reader_nas.h"
#include "gcache_tmp_writer_nas.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class ReaderNAS;
class WriterNAS;

class Reader;
class Writer;
class KVTable;
struct MetaTMPNAS;
class Storage;

/**
 * FileNAS class is a child of File.
 * this class provides some functions of file in NAS.
 */
class FileNAS: public File {
public:

    /**
     * destructor, destruct object.
     */
    virtual ~FileNAS();

    /**
     * set the ReaderNAS object according to advice,then return the pointer of object.
     */
    virtual ReaderNAS* OpenReader(FAdvice advice = FADV_NONE);

    /**
     * get the file descriptor then use it to construct a WriterNAS object,then return the pointer of object.
     */
    virtual WriterNAS* OpenWriter(lifetime_t lifetime_days = LIFETIME_DEFAULT, CacheLevel cache_lv =
            CACHE_L3, FAdvice advice = FADV_NONE);

    /**
     * get the size of this file, in bytes.
     */
    virtual int64_t Size() const;

    /**
     * get the life time of this file, in days.
     */
    virtual lifetime_t Lifetime() const;

    /**
     * determine whether this file exists.
     */
    virtual bool Exists() const;

    /**
     * remove this file exists.
     */
    virtual bool Remove();

    /**
     * set the life time of this file, in days.
     */
    virtual bool SetLifetime(lifetime_t lifetime_days);

    /**
     * rename the file old name to new name.
     */
    virtual bool Rename(const std::string &new_name);

    /**
     * copy this file to cache level which parameter cache_lv indicates.
     */
    virtual bool Copy(const std::string& new_name, CacheLevel cache_lv);

    /**
     * copy this file to the same cache level.
     */
    virtual bool Copy(const std::string& new_name);

    /**
     * make a new hard link to the existing file named by old name.
     */
    virtual bool Link(const std::string &link_name);

    /**
     * changes the size of filename to length.
     */
    virtual bool Truncate(int64_t size);

    /**
     * get the time of last modification, in milliseconds.
     */
    virtual uint64_t LastModified() const;

    /**
     * get the name of this file.
     */
    virtual std::string GetName() const;

    /**
     * determine whether this file is expired or not.
     */
    virtual bool LifetimeExpired() const;

    /**
     * set lock.
     */
    virtual void Lock();

    /**
     * set unlock.
     */
    virtual void Unlock();

    friend class File;

private:

    /**
     * constructor, initialize member.
     */
    FileNAS(const std::string &file_name);

    virtual bool Init();                           //initialize member.
    MetaTMPNAS *GetMeta() const;                   //get meta data of this file
    std::string GetRealFilename() const; //get the real file name of this file, this member contains root directory.

    boost::shared_ptr<bool> _isWriterAlive;        //indicate if this writer is alive
    std::string _filename;     //file name of this file, this member doesn't contain root directory.
    Storage* _storage;                             //member storage, used for meta data operation.
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_FILE_CENTRAL_H_ */
