#ifndef GCACHE_SEIS_STORAGE_H_
#define GCACHE_SEIS_STORAGE_H_

#include <hdfs.h>
#include <pthread.h>
#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_error.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_log.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
class StorageSeis {
public:
    static StorageSeis* GetInstance();
    ~StorageSeis();
    bool PutMeta(const MetaSeisHDFS* metaData, const std::string& data_name);
    MetaSeisHDFS* GetMeta(const std::string& data_name);
    hdfsFS ReturnFS();
    bool FixFileTrace(const std::string& data_name, const MetaSeisHDFS* metaData);
    bool FixFileHead(const std::string& data_name, const MetaSeisHDFS* metaData);
    std::string ReturnRootDir();

    class CGarbor {
    public:
        ~CGarbor();
    };

private:
    StorageSeis();
    bool Init();
    static StorageSeis* _storageSeis;
    static pthread_mutex_t _seis_mutex;
    static std::string _root_dir;
    static hdfsFS _fs;
    static CGarbor _garbor;
};
NAMESPACE_END_SEISFS
}
#endif
