/*
 * gcache_seis_reel.h
 *
 *  Created on: Jul 5, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_VOLHEADER_H_
#define GCACHE_SEIS_VOLHEADER_H_

#include "gcache_global.h"
#include "util/seisfs_shared_pointer.h"

#include <stdint.h>
#include "gcache_seis_meta.h"
#include "seisfile_kvinfo.h"
#include <hdfs.h>
#include <string>
NAMESPACE_BEGIN_SEISFS
namespace file {
#define OBJ_DIR_NUM                100

class SeisKVInfo: public KVInfo {
public:

    virtual ~SeisKVInfo();

    virtual bool Put(const std::string &key, const char* data, uint32_t size);

    virtual bool Delete(const std::string &key);

    virtual bool Get(const std::string &key, char*& data, uint32_t& size);

    virtual uint32_t SizeOf(const std::string &key);

    virtual bool Exists(const std::string &key);

    virtual bool GetKeys(std::vector<std::string> &Keys);

    friend class HDFS;

private:

    SeisKVInfo(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta);

    bool Init();

    virtual std::string GetObjFilename(const std::string &key);

    std::string _hdfs_data_name;
    std::string _hdfs_data_reel_name;
    hdfsFS _fs;
    SharedPtr<MetaSeisHDFS> _meta;

    bool _is_reel_modified;
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_VOLHEADER_H_ */
