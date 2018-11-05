#include <cerrno>

#include "gcache_seis_storage.h"
#include "gcache_io.h"
#include "gcache_seis_utils.h"

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
StorageSeis::StorageSeis() {
}

StorageSeis* StorageSeis::GetInstance() {
    if (_storageSeis == NULL) {
        pthread_mutex_lock(&_seis_mutex);
        _storageSeis = new StorageSeis();
        if (!_storageSeis->Init()) {
            delete _storageSeis;
            _storageSeis = NULL;
            pthread_mutex_unlock(&_seis_mutex);
            return NULL;
        }
        pthread_mutex_unlock(&_seis_mutex);
    }
    return _storageSeis;
}

StorageSeis::~StorageSeis() {
    if (_fs != NULL) {
        hdfsDisconnect(_fs);
        _fs = NULL;
    }
}

bool StorageSeis::Init() {
    char *seisRootDir = getenv("SEIS_ROOT_DIR");
    if (seisRootDir == NULL) {
        Err("envionment variable SEIS_ROOT_DIR is NULL");
        GCACHE_seis_errno = GCACHE_SEIS_ERR_ROOTDIR;
        return false;
    }

    _root_dir = seisRootDir;
    _fs = getHDFS(_root_dir);
    if (_fs == NULL) {
        Err("getHDFS fail");
        GCACHE_seis_errno = GCACHE_SEIS_ERR_GETFS;
        return false;
    }
    return true;
}

bool StorageSeis::PutMeta(const MetaSeisHDFS* metaData, const std::string& data_name) {
    if (metaData == NULL) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
        return false;
    }

    std::string _hdfs_meta_name = hdfsGetPath(_root_dir + "/" + data_name) + "/" + "seis.meta";

    //O_WRONLY mean write,create or truncate
    hdfsFile fd = hdfsOpenFile(_fs, _hdfs_meta_name.c_str(), O_WRONLY, 0,
    REPLICATION, 0);
    if (NULL == fd) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }
    hdfsChmod(_fs, _hdfs_meta_name.c_str(), 0644);

    int write_len = sizeof(MetaSeisHDFS);
    if (write_len != hdfsWrite(_fs, fd, metaData, write_len)) {
        hdfsCloseFile(_fs, fd);
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }

    if (metaData->head_sizes != NULL) {
        write_len = metaData->head_length * sizeof(*metaData->head_sizes);
        if (write_len != hdfsWrite(_fs, fd, metaData->head_sizes, write_len)) {
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
            return false;
        }
    }

    //--------------ice
    if (metaData->trace_dir_size > 0) {
        for (int64_t i = 0; i < metaData->trace_dir_size; ++i) {
            int uuid_len = UUID_LENGTH;
            if (uuid_len != hdfsWrite(_fs, fd, metaData->trace_dir_uuid_array[i], uuid_len)) {
                hdfsCloseFile(_fs, fd);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
                return false;
            }
        }

        write_len = metaData->trace_dir_size * sizeof(int64_t);
        if (write_len != SafeWrite(_fs, fd, (char*) metaData->trace_dir_rows_array, write_len)) {
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
            return false;
        }

        write_len = metaData->trace_dir_size * sizeof(int64_t);
        if (write_len != SafeWrite(_fs, fd, (char*) metaData->trace_dir_front_array, write_len)) {
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
            return false;
        }
    }

    if (metaData->head_dir_size > 0) {
        for (int64_t i = 0; i < metaData->head_dir_size; ++i) {
            int uuid_len = UUID_LENGTH;
            if (uuid_len != hdfsWrite(_fs, fd, metaData->head_dir_uuid_array[i], uuid_len)) {
                hdfsCloseFile(_fs, fd);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
                return false;
            }
        }

        write_len = metaData->head_dir_size * sizeof(int64_t);
        if (write_len != SafeWrite(_fs, fd, (char*) metaData->head_dir_rows_array, write_len)) {
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
            return false;
        }

        write_len = metaData->head_dir_size * sizeof(int64_t);
        if (write_len != SafeWrite(_fs, fd, (char*) metaData->head_dir_front_array, write_len)) {
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
            return false;
        }
    }
    //----------------------

    hdfsSync(_fs, fd);
    if (hdfsCloseFile(_fs, fd) != 0) {
        return false;
    }
    return true;
}

bool StorageSeis::FixFileTrace(const std::string& hdfs_data_name, const MetaSeisHDFS* metaData) {
    /*
     * check file number
     * by zhounan
     */
    std::string src_last_trace_dir = hdfs_data_name + "/"
            + metaData->trace_dir_uuid_array[metaData->trace_dir_size - 1];
    int64_t total_file_size = GetTotalFileSize(_fs, src_last_trace_dir + "/" + "Write_Data");
    if (total_file_size % metaData->trace_row_bytes != 0
            && hdfsExists(_fs, (src_last_trace_dir + "/" + "Write_Data").c_str()) == 0
            && !TruncateToNeatFile(_fs, src_last_trace_dir + "/" + "Write_Data",
                    metaData->trace_row_bytes)) {
        return false;
    }

    return true;
    /*
     * end
     */

}

bool StorageSeis::FixFileHead(const std::string& hdfs_data_name, const MetaSeisHDFS* metaData) {
    /*
     * check file number
     * by zhounan
     */
    std::string src_last_head_dir = hdfs_data_name + "/"
            + metaData->head_dir_uuid_array[metaData->head_dir_size - 1];
    int64_t total_file_size = GetTotalFileSize(_fs, src_last_head_dir + "/" + "Write_Data");
    if (metaData->head_placement == BY_ROW) {
        if (total_file_size % metaData->head_row_bytes != 0
                && !TruncateToNeatFile(_fs, src_last_head_dir + "/" + "Write_Data",
                        metaData->head_row_bytes)) {
            return false;
        }
    } else {  //BY_COLUMN
        int64_t cache_size = metaData->head_row_bytes * metaData->column_turn;
        if (total_file_size % cache_size != 0) {
            if (hdfsExists(_fs, (src_last_head_dir + "/" + "Write_Data").c_str()) == 0
                    && !TruncateToNeatFile(_fs, src_last_head_dir + "/" + "Write_Data",
                            cache_size)) {
                return false;
            }
            if (hdfsExists(_fs, (src_last_head_dir + "/" + "Cache_Data").c_str()) == 0
                    && !TruncateToNeatFile(_fs, src_last_head_dir + "/" + "Cache_Data",
                            2 * (cache_size + sizeof(int64_t)))) {
                return false;
            }
        } else {
            if (hdfsExists(_fs, (src_last_head_dir + "/" + "Cache_Data").c_str()) == 0
                    && !TruncateToNeatFile(_fs, src_last_head_dir + "/" + "Cache_Data",
                            cache_size + sizeof(int64_t))) {
                return false;
            }
        }
    }

    return true;
    /*
     * end
     */

}

MetaSeisHDFS* StorageSeis::GetMeta(const std::string& data_name) {

    std::string hdfs_data_name = hdfsGetPath(_root_dir + "/" + data_name);
    std::string _hdfs_meta_name = hdfs_data_name + "/" + "seis.meta";
    hdfsFile fd = hdfsOpenFile(_fs, _hdfs_meta_name.c_str(), O_RDONLY, 0,
    REPLICATION, 0);

    if (NULL == fd) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return NULL;
    }
    int read_len = sizeof(MetaSeisHDFS);
    struct MetaSeisHDFS* metaData = new MetaSeisHDFS();
    if (read_len != hdfsRead(_fs, fd, metaData, read_len)) {
        delete metaData;
        hdfsCloseFile(_fs, fd);
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
        return NULL;
    }

    //clear all pointers
    metaData->head_sizes = NULL;
    metaData->trace_dir_uuid_array = NULL;
    metaData->trace_dir_rows_array = NULL;
    metaData->trace_dir_front_array = NULL;
    metaData->head_dir_uuid_array = NULL;
    metaData->head_dir_rows_array = NULL;
    metaData->head_dir_front_array = NULL;

    if (metaData->head_length > 0) {
        read_len = metaData->head_length * sizeof(*metaData->head_sizes);
        metaData->head_sizes = new int[metaData->head_length];
        int64_t ret_len = SafeRead(_fs, fd, metaData->head_sizes, read_len);

        if (ret_len != read_len) {
            delete metaData;
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return NULL;
        }
    }

    //-------------------ice
    if (metaData->trace_dir_size > 0) {
        metaData->trace_dir_uuid_array = new char*[metaData->trace_dir_size];
        for (int64_t i = 0; i < metaData->trace_dir_size; ++i) {
            int uuid_len = UUID_LENGTH;
            metaData->trace_dir_uuid_array[i] = new char[uuid_len];
            if (uuid_len != hdfsRead(_fs, fd, metaData->trace_dir_uuid_array[i], uuid_len)) {
                delete metaData;
                hdfsCloseFile(_fs, fd);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
                return NULL;
            }
        }

        read_len = metaData->trace_dir_size * sizeof(int64_t);
        metaData->trace_dir_rows_array = new int64_t[metaData->trace_dir_size];
        if (read_len != SafeRead(_fs, fd, metaData->trace_dir_rows_array, read_len)) {
            delete metaData;
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return NULL;
        }

        std::string src_last_trace_dir = hdfs_data_name + "/"
                + metaData->trace_dir_uuid_array[metaData->trace_dir_size - 1];
        int64_t total_file_size = GetTotalFileSize(_fs, src_last_trace_dir + "/" + "Write_Data");
        if (total_file_size == -1) {
            delete metaData;
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY,
            errno);
            return NULL;
        }
        int64_t last_trace_dir_idx = metaData->trace_dir_size - 1;
        metaData->trace_dir_rows_array[last_trace_dir_idx] = total_file_size
                / metaData->trace_row_bytes;

        read_len = metaData->trace_dir_size * sizeof(int64_t);
        metaData->trace_dir_front_array = new int64_t[metaData->trace_dir_size];
        if (read_len != SafeRead(_fs, fd, metaData->trace_dir_front_array, read_len)) {
            delete metaData;
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return NULL;
        }

        metaData->trace_total_rows = metaData->trace_dir_front_array[last_trace_dir_idx]
                + metaData->trace_dir_rows_array[last_trace_dir_idx];
    }

    if (metaData->head_dir_size > 0) {
        metaData->head_dir_uuid_array = new char*[metaData->head_dir_size];
        for (int64_t i = 0; i < metaData->head_dir_size; ++i) {
            int uuid_len = UUID_LENGTH;
            metaData->head_dir_uuid_array[i] = new char[uuid_len];
            if (uuid_len != hdfsRead(_fs, fd, metaData->head_dir_uuid_array[i], uuid_len)) {
                delete metaData;
                hdfsCloseFile(_fs, fd);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
                return NULL;
            }
        }

        read_len = metaData->head_dir_size * sizeof(int64_t);
        metaData->head_dir_rows_array = new int64_t[metaData->head_dir_size];
        if (read_len != SafeRead(_fs, fd, metaData->head_dir_rows_array, read_len)) {
            delete metaData;
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return NULL;
        }

        std::string src_last_head_dir = hdfs_data_name + "/"
                + metaData->head_dir_uuid_array[metaData->head_dir_size - 1];
        int64_t total_file_size = GetTotalFileSize(_fs, src_last_head_dir + "/" + "Write_Data"); //need to add Cache_Data  -----------ice

        // Cache
        int64_t cache_num = 0;
        if (metaData->head_placement == BY_COLUMN) {
            std::string hdfs_cache_name = src_last_head_dir + "/Cache_Data/Cache";
            if (hdfsExists(_fs, hdfs_cache_name.c_str()) == 0) {
                hdfsFile cache_fd = hdfsOpenFile(_fs, hdfs_cache_name.c_str(), O_RDONLY, 0,
                REPLICATION, 0);
                if (cache_fd == NULL
                        || sizeof(int64_t)
                                != hdfsRead(_fs, cache_fd, &cache_num, sizeof(int64_t))) {
                    delete metaData;
                    hdfsCloseFile(_fs, cache_fd);
                    hdfsCloseFile(_fs, fd);
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
                    return NULL;
                }
                if (cache_num <= 0 || cache_num > metaData->column_turn) {
                    delete metaData;
                    hdfsCloseFile(_fs, cache_fd);
                    hdfsCloseFile(_fs, fd);
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY,
                    errno);
                    return NULL;
                }
                hdfsCloseFile(_fs, cache_fd);
            }
        }

        if (total_file_size == -1) {
            delete metaData;
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY,
            errno);
            return NULL;
        }
        int64_t last_head_dir_idx = metaData->head_dir_size - 1;
        metaData->head_dir_rows_array[last_head_dir_idx] = cache_num
                + total_file_size / metaData->head_row_bytes;

        read_len = metaData->head_dir_size * sizeof(int64_t);
        metaData->head_dir_front_array = new int64_t[metaData->head_dir_size];
        if (read_len != SafeRead(_fs, fd, metaData->head_dir_front_array, read_len)) {
            delete metaData;
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return NULL;
        }

        metaData->head_total_rows = metaData->head_dir_front_array[last_head_dir_idx]
                + metaData->head_dir_rows_array[last_head_dir_idx];
    }
    //-------------------
    hdfsCloseFile(_fs, fd);

    /*
     * checking update file
     * by zhounan
     */
    //check head
    for (int i = 0; i < metaData->head_dir_size; i++) {

        std::string src_dir = hdfs_data_name + "/" + metaData->head_dir_uuid_array[i];
        if (!TruncateUpdateToNeatFile(_fs, src_dir, metaData->head_row_bytes)) {
            delete metaData;
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_GETMETA, errno);
            return NULL;
        }
    }
    //check trace
    for (int i = 0; i < metaData->trace_dir_size; i++) {
        std::string src_dir = hdfs_data_name + "/" + metaData->trace_dir_uuid_array[i];
        if (!TruncateUpdateToNeatFile(_fs, src_dir, metaData->trace_row_bytes)) {
            delete metaData;
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_GETMETA, errno);
            return NULL;
        }
    }

    /*
     * end
     */

    return metaData;

}

hdfsFS StorageSeis::ReturnFS() {
    return _fs;
}

std::string StorageSeis::ReturnRootDir() {
    return _root_dir;
}

StorageSeis::CGarbor::~CGarbor() {
    if (StorageSeis::_storageSeis) {
        delete StorageSeis::_storageSeis;
        StorageSeis::_storageSeis = NULL;
    }

}

StorageSeis* StorageSeis::_storageSeis = NULL;
pthread_mutex_t StorageSeis::_seis_mutex;
std::string StorageSeis::_root_dir = "";
hdfsFS StorageSeis::_fs = NULL;

NAMESPACE_END_SEISFS
}
