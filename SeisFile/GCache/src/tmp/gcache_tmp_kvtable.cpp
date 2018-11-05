#include "gcache_tmp_kvtable.h"

#include <cerrno>
#include <string>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_kvtable_hdfs.h"
#include "gcache_tmp_kvtable_local.h"
#include "gcache_tmp_kvtable_nas.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
KVTable* KVTable::New(const std::string &table_name, StorageArch &s_arch) {
    KVTable *kv_table = NULL;
    switch (s_arch) {
        case STORAGE_LOCAL:
            kv_table = new KVTableLocal(table_name);
            break;
        case STORAGE_DIST:
            kv_table = new KVTableHDFS(table_name);
            break;
        case STORAGE_NAS:
            kv_table = new KVTableNAS(table_name);
            break;
        default:
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOARCH;
            return NULL;
    }

    if (!kv_table->Init()) {
        delete kv_table;
        if (s_arch == STORAGE_DIST) {
            kv_table = new KVTableNAS(table_name);
            if (kv_table->Init()) {
                s_arch = STORAGE_NAS;
                return kv_table;
            }
        }
        return NULL;
    }
    return kv_table;

}

NAMESPACE_END_SEISFS
}
