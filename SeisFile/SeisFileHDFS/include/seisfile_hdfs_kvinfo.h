/*
 * seisfile_kvinfo.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_HDFS_KVINFO_H_
#define SEISFILE_HDFS_KVINFO_H_

#include <stdint.h>
#include <string>
#include <vector>
#include "seisfile_kvinfo.h"
#include <GEFile.h>
#include <map>

namespace seisfs {

namespace file {

class KVInfoHDFS : public KVInfo {
public:

	KVInfoHDFS(const std::string& filename);

    virtual ~KVInfoHDFS();

    bool Put(const std::string &key, const char* data, uint32_t size);

    bool Delete(const std::string &key);

    bool Get(const std::string &key, char*& data, uint32_t& size);

    uint32_t SizeOf(const std::string &key);

    bool Exists(const std::string &key);

    bool GetKeys(std::vector<std::string> &Keys);

    bool Init();

private:

    void KV_MetaRead();
    void KV_MetaWrite(uint32_t key_length, uint32_t value_length);
    void KV_Update();
    void KV_GetCurrentMetaInfo();
    void KV_UpdateCurrentMetaInfo();

    map<string, string> _kv_map;
    map<string, uint32_t> _kv_start_pos;

	uint32_t _kvinfo_num;
	uint32_t _valid_kvinfo_num;

    std::string _filename;
    std::string _kvinfo_filename;
    std::string _key_filename, _value_filename, _meta_filename;
//    const std::string SEISHDFS_HOME_PATH = "/d0/data/seisfiletest/seisfiledisk/kvinfo/";
    std::string seisnas_home_path_;
//    GEFile *_gf_kvinfo_key,*_gf_kvinfo_value,*_gf_kvmeta;

};

}

}

#endif /* SEISFILE_HDFS_KVINFO_H_ */
