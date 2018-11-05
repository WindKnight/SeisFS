/*
 * gcache_seis_data.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */
#include "gcache_seis_headreader_column.h"
#include "gcache_seis_data.h"
#include <sys/time.h>
#include "gcache_seis_error.h"
#include "gcache_string.h"
#include "util/seisfs_scoped_pointer.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_utils.h"
#include "gcache_seis_headupdater_row.h"
#include "gcache_seis_headupdater_column.h"
#include "gcache_log.h"
#include "gcache_io.h"
#include "gcache_seis_headreader_row.h"
#include "gcache_seis_headwriter_row.h"
#include "gcache_seis_headwriter_column.h"
#include "gcache_seis_storage.h"
#include "string.h"
#include "gcache_seis_tracecompactor.h"
#include "gcache_seis_headcompactor_row.h"
#include "gcache_seis_headcompactor_column.h"
#include "gcache_seis_merger.h"
#include "gcache_seis_truncater.h"
#include <uuid/uuid.h>

NAMESPACE_BEGIN_SEISFS
namespace file {
SeisFile* HDFS::New(const std::string& data_name) {

    if (data_name.length() == 0 || data_name[0] != '/') {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }

    HDFS *seisFileHDFS = new HDFS(data_name);
    if (!seisFileHDFS->Init()) {
        delete seisFileHDFS;
        return NULL;
    }
    return seisFileHDFS;
}

//bool SeisFileHDFS::Merge(const std::string &strNewName,const std::vector<std::string>& src_data_names){
//    if(src_data_names.size()<=0){
//        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_PARAMERR, errno);
//        return false;
//    }
//    SeisFileHDFS* seis = New(src_data_names[0]);
//    for(unsigned int i=1;i<src_data_names.size();i++){
//        if(!seis->Merge(src_data_names[i])){
//            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_MERGE_FAULT, errno);
//            return false;
//        }
//    }
//    seis->Rename(strNewName);
//    delete seis;
//    return true;
//}

HDFS::HDFS(const std::string& data_name) {
    _data_name = data_name;
    _hdfs_data_name = "";
    _hdfs_meta_name = "";
    _storage_seis = NULL;
//    _meta = NULL;
    _fs = NULL;
}

bool HDFS::Init() {
    _storage_seis = StorageSeis::GetInstance();
    if (_storage_seis == NULL) {
        return false;
    }

    _root_dir = _storage_seis->ReturnRootDir();
    _hdfs_data_name = hdfsGetPath(_root_dir) + _data_name;
    _cur_hdfs_data_name = _hdfs_data_name;
    _hdfs_meta_name = _hdfs_data_name + "/" + "seis.meta";
    _fs = _storage_seis->ReturnFS();

    _meta = Exists() ? _storage_seis->GetMeta(_data_name) : new MetaSeisHDFS();

    if (_meta == NULL) {
        return false;
    }
    return true;
}

HDFS::~HDFS() {
//    if(_storage_seis != NULL){
////        StorageSeis::DeleteInstance();
//        delete _storage_seis;
//        _storage_seis = NULL;
//    }
}

bool HDFS::SetSortKeys(const SortKeys& sort_keys) {

    //----------merge sort keys
    SeisKey* data = new SeisKey[sort_keys.size()];
    int i = 0;
    for (std::vector<SeisKey>::const_iterator it = sort_keys.begin(); it != sort_keys.end(); it++) {
        data[i++] = *it;
    }

    std::string hdfs_sort_key_name = _hdfs_data_name + "/" + "Seis.SortKey";
    hdfsFile sort_key_fd = hdfsOpenFile(_fs, hdfs_sort_key_name.c_str(),
    O_WRONLY | O_APPEND, 0, REPLICATION, 0);
    if (sort_key_fd == NULL) {
        delete[] data;
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }
    hdfsChmod(_fs, hdfs_sort_key_name.c_str(), 0644);

    unsigned int data_size = sort_keys.size() * sizeof(SeisKey);
    int64_t ret_data_len = 0;
    ret_data_len = SafeWrite(_fs, sort_key_fd, (char*) data, data_size);
    delete[] data;
    if (hdfsCloseFile(_fs, sort_key_fd) == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
        return false;
    }

    if (ret_data_len != data_size) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }

    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return true;

}

SortKeys HDFS::GetSortKeys() {
    SortKeys sort_keys;
    std::string hdfs_sort_key_name = _hdfs_data_name + "/" + "Seis.SortKey";
    hdfsFile sort_key_fd = hdfsOpenFile(_fs, hdfs_sort_key_name.c_str(),
    O_RDONLY, 0, REPLICATION, 0);

    if (sort_key_fd != NULL) {
        hdfsFileInfo * fileInfo = hdfsGetPathInfo(_fs, hdfs_sort_key_name.c_str());
        if (fileInfo == NULL) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_STAT, errno);
            hdfsCloseFile(_fs, sort_key_fd);
            return sort_keys;
        }
        int64_t data_size = fileInfo->mSize;
        hdfsFreeFileInfo(fileInfo, 1);

        char *readData = new char[data_size];
        if (data_size != SafeRead(_fs, sort_key_fd, readData, data_size)) {
            hdfsCloseFile(_fs, sort_key_fd);
            delete[] readData;
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return sort_keys;
        }

        SeisKey *sortKeyDat = (SeisKey*) readData;
        for (uint64_t i = 0; i < data_size / sizeof(SeisKey); i++) {
            sort_keys.push_back(*(sortKeyDat + i));
        }

        delete[] readData;
    } else {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return sort_keys;
    }

    if (hdfsCloseFile(_fs, sort_key_fd) != 0) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
        return sort_keys;
    }
    TouchSeisAccessTime(_fs, _hdfs_data_name);
    return sort_keys;
}

/**
 * Like k-v table. Reel filename format: {data_name}.{key}.reel
 */
KVInfo* HDFS::OpenKVInfo() {
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    SeisKVInfo* reel = new SeisKVInfo(_fs, _hdfs_data_name, _meta);
    if (!reel->Init()) {
        delete reel;
        return NULL;
    }

    TouchSeisAccessTime(_fs, _hdfs_data_name);
    return reel;
}

/*
 * Read only
 */
HeadReader* HDFS::OpenHeadReader() {
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    switch (_meta->head_placement) {
        case BY_ROW: {
            SeisHeadReaderRow* headReaderRow = new SeisHeadReaderRow(_fs, _hdfs_data_name, _meta);
            if (!headReaderRow->Init()) {
                delete headReaderRow;
                return NULL;
            }
            TouchSeisAccessTime(_fs, _hdfs_data_name);
            return headReaderRow;
        }
            break;
        case BY_COLUMN: {
            SeisHeadReaderColumn* headReaderColumn = new SeisHeadReaderColumn(_fs, _hdfs_data_name,
                    _meta);
            if (!headReaderColumn->Init()) {
                delete headReaderColumn;
                return NULL;
            }
            TouchSeisAccessTime(_fs, _hdfs_data_name);
            return headReaderColumn;
        }
            break;
        default: {
            return NULL;
        }
            break;
    }
}

TraceReader* HDFS::OpenTraceReader() {
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    SeisTraceReader* traceReader = new SeisTraceReader(_fs, _hdfs_data_name, _meta);
    if (!traceReader->Init()) {
        delete traceReader;
        return NULL;
    }
    TouchSeisAccessTime(_fs, _hdfs_data_name);
    return traceReader;
}

Reader* HDFS::OpenReader() {
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    SeisReader* reader = new SeisReader(_fs, _hdfs_data_name, _meta);
    if (!reader->Init()) {
        delete reader;
        return NULL;
    }
    TouchSeisAccessTime(_fs, _hdfs_data_name);
    return reader;
}

/*
 * Append only
 */
HeadWriter* HDFS::OpenHeadWriter(const HeadType& headType, Lifetime lifetime_days) {
    if (headType.head_size.size() == 0 && !Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }

    lifetime_days = lifetime_days > LIFETIME_DEFAULT ? LIFETIME_DEFAULT : lifetime_days; // not bigger than 1 month
    if (_meta->head_length == 0 && headType.head_size.size() != 0) {
        int64_t now = GetCurrentMillis();
        _meta->creation_time = now;
        _meta->head_length = headType.head_size.size();
        _meta->head_placement = DEFAULT_PLACEMENT;
        _meta->meta.lifetime = lifetime_days;

        if (_meta->column_turn <= 0) {
            _meta->column_turn = COLUMN_TURN;
            _meta->file_max_rows = FILE_MAX_ROWS_TIMES * COLUMN_TURN;

            //------------zhounan
            _meta->update_block_num = _meta->file_max_rows * UPDATE_BLOCK_ROW_TIMES;
            _meta->update_block_size = _meta->update_block_num * _meta->trace_type_size;
        }

        _meta->head_sizes = new int[headType.head_size.size()];
        for(unsigned int i=0;i<headType.head_size.size();i++){
            _meta->head_sizes[i] = headType.head_size[i];
        }

        //----------------ice
        _meta->head_row_bytes = GetArraySum(_meta->head_sizes, headType.head_size.size());
        _meta->head_dir_size = 1;
        _meta->head_total_rows = 0;
        _meta->head_dir_uuid_array = new char*[1] { GetNewUuid() };
        _meta->head_dir_rows_array = new int64_t[1] { 0 };
        _meta->head_dir_front_array = new int64_t[1] { 0 };
        if (!CreateAllDir(_fs, _hdfs_data_name, _meta->head_dir_uuid_array[0])) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR, errno);
            return NULL;
        }

        if (_storage_seis->PutMeta(_meta, _data_name) == false) {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
            return NULL;
        }
    }

    if (_meta->head_length != headType.head_size.size() || _meta->head_placement != DEFAULT_PLACEMENT) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }

    if (!_storage_seis->FixFileHead(_hdfs_data_name, _meta)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }
    switch (_meta->head_placement) {
        case BY_ROW: {
            SeisHeadWriterRow* headWriterRow = new SeisHeadWriterRow(_fs, _hdfs_data_name, _meta,
                    _storage_seis, _data_name);
            if (!headWriterRow->Init()) {
                delete headWriterRow;
                return NULL;
            }

            TouchSeisModifyTime(_fs, _hdfs_data_name);
            return headWriterRow;
        }
            break;
        case BY_COLUMN: {
            SeisHeadWriterColumn* headwriterColumn = new SeisHeadWriterColumn(_fs, _hdfs_data_name,
                    _meta, _storage_seis, _data_name);
            if (!headwriterColumn->Init()) {
                delete headwriterColumn;
                return NULL;
            }

            TouchSeisModifyTime(_fs, _hdfs_data_name);
            return headwriterColumn;
        }
            break;
        default: {
            return NULL;
        }
            break;
    }
}

TraceWriter* HDFS::OpenTraceWriter(const TraceType& traceType, Lifetime lifetime_days) {
    if (traceType.trace_size == 0 || !Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }

    //进行数据完整性的检查
    lifetime_days = lifetime_days > LIFETIME_DEFAULT ? LIFETIME_DEFAULT : lifetime_days; // not bigger than 1 month
    if (_meta->trace_length == 0 && traceType.trace_size != 0) {
        int64_t now = GetCurrentMillis();
        _meta->creation_time = now;
        _meta->trace_length = traceType.trace_size;
        _meta->meta.lifetime = lifetime_days;

        if (_meta->column_turn <= 0) {
            _meta->column_turn = COLUMN_TURN;
            _meta->file_max_rows = FILE_MAX_ROWS_TIMES * COLUMN_TURN;

            //------------zhounan
            _meta->update_block_num = _meta->file_max_rows * UPDATE_BLOCK_ROW_TIMES;
            _meta->update_block_size = _meta->update_block_num * _meta->trace_type_size;
        }
        //----------ice
        _meta->trace_row_bytes = traceType.trace_size * _meta->trace_type_size;
        _meta->trace_dir_size = 1;
        _meta->trace_total_rows = 0;
        _meta->trace_dir_uuid_array = new char*[1] { GetNewUuid() };
        _meta->trace_dir_rows_array = new int64_t[1] { 0 };
        _meta->trace_dir_front_array = new int64_t[1] { 0 };
        if (!CreateAllDir(_fs, _hdfs_data_name, _meta->trace_dir_uuid_array[0])) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR, errno);
            return NULL;
        }

        if (_storage_seis->PutMeta(_meta, _data_name) == false) {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
            return NULL;
        }
    }

    if (_meta->trace_length != traceType.trace_size) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }

    if (!_storage_seis->FixFileTrace(_hdfs_data_name, _meta)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }

    SeisTraceWriter* traceWriter = new SeisTraceWriter(_fs, _hdfs_data_name, _meta, _storage_seis,
            _data_name);
    if (!traceWriter->Init()) {
        delete traceWriter;
        return NULL;
    }
    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return traceWriter;
}

Writer* HDFS::OpenWriter(const HeadType& headType, const TraceType& traceType,
        Lifetime lifetime_days) {
    if (!Exists() && (headType.head_size.size() == 0 ||  traceType.trace_size == 0)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }

    // wrong if open head then open writer
    lifetime_days = lifetime_days > LIFETIME_DEFAULT ? LIFETIME_DEFAULT : lifetime_days; // not bigger than 1 month
    if (!Exists()) {

        int64_t now = GetCurrentMillis();
        _meta->creation_time = now;
        _meta->head_length = headType.head_size.size();
        _meta->head_placement = DEFAULT_PLACEMENT;
        _meta->trace_length = traceType.trace_size;
        _meta->meta.lifetime = lifetime_days;

        if (_meta->column_turn <= 0) {
            _meta->column_turn = COLUMN_TURN;
            _meta->file_max_rows = FILE_MAX_ROWS_TIMES * COLUMN_TURN;

            //------------zhounan
            _meta->update_block_num = _meta->file_max_rows * UPDATE_BLOCK_ROW_TIMES;
            _meta->update_block_size = _meta->update_block_num * _meta->trace_type_size;
        }
        _meta->head_sizes = new int[headType.head_size.size()];
        for(unsigned int i=0;i<headType.head_size.size();i++){
            _meta->head_sizes[i] = headType.head_size[i];
        }

        //-----------ice
        _meta->head_row_bytes = GetArraySum(_meta->head_sizes, headType.head_size.size());

        _meta->head_dir_size = 1;
        _meta->head_total_rows = 0;
        _meta->head_dir_uuid_array = new char*[1] { GetNewUuid() };
        _meta->head_dir_rows_array = new int64_t[1] { 0 };
        _meta->head_dir_front_array = new int64_t[1] { 0 };
        if (!CreateAllDir(_fs, _hdfs_data_name, _meta->head_dir_uuid_array[0])) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR, errno);
            return NULL;
        }

        _meta->trace_row_bytes = traceType.trace_size * _meta->trace_type_size;
        _meta->trace_dir_size = 1;
        _meta->trace_total_rows = 0;
        _meta->trace_dir_uuid_array = new char*[1] { GetNewUuid() };
        _meta->trace_dir_rows_array = new int64_t[1] { 0 };
        _meta->trace_dir_front_array = new int64_t[1] { 0 };
        if (!CreateAllDir(_fs, _hdfs_data_name, _meta->trace_dir_uuid_array[0])) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR, errno);
            return NULL;
        }

        if (_storage_seis->PutMeta(_meta, _data_name) == false) {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
            return NULL;
        }
    }
    if (_meta->trace_length != traceType.trace_size || _meta->head_length != headType.head_size.size()
    || _meta->head_placement != DEFAULT_PLACEMENT) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }
    if (!(_storage_seis->FixFileHead(_hdfs_data_name, _meta)
            && _storage_seis->FixFileTrace(_hdfs_data_name, _meta))) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }
    SeisWriter* writer = new SeisWriter(_fs, _hdfs_data_name, _meta, _storage_seis, _data_name);
    if (!writer->Init()) {
        delete writer;
        return NULL;
    }
    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return writer;
}

HeadWriter* HDFS::OpenHeadWriter(){
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    if (!_storage_seis->FixFileHead(_hdfs_data_name, _meta)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }
    switch (_meta->head_placement) {
        case BY_ROW: {
            SeisHeadWriterRow* headWriterRow = new SeisHeadWriterRow(_fs, _hdfs_data_name, _meta,
                    _storage_seis, _data_name);
            if (!headWriterRow->Init()) {
                delete headWriterRow;
                return NULL;
            }

            TouchSeisModifyTime(_fs, _hdfs_data_name);
            return headWriterRow;
        }
            break;
        case BY_COLUMN: {
            SeisHeadWriterColumn* headwriterColumn = new SeisHeadWriterColumn(_fs, _hdfs_data_name,
                    _meta, _storage_seis, _data_name);
            if (!headwriterColumn->Init()) {
                delete headwriterColumn;
                return NULL;
            }

            TouchSeisModifyTime(_fs, _hdfs_data_name);
            return headwriterColumn;
        }
            break;
        default: {
            return NULL;
        }
            break;
    }
}
TraceWriter* HDFS::OpenTraceWriter(){
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    if (!_storage_seis->FixFileTrace(_hdfs_data_name, _meta)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }

    SeisTraceWriter* traceWriter = new SeisTraceWriter(_fs, _hdfs_data_name, _meta, _storage_seis,
            _data_name);
    if (!traceWriter->Init()) {
        delete traceWriter;
        return NULL;
    }
    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return traceWriter;
}
Writer* HDFS::OpenWriter(){
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    if (!(_storage_seis->FixFileHead(_hdfs_data_name, _meta)
            && _storage_seis->FixFileTrace(_hdfs_data_name, _meta))) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return NULL;
    }
    SeisWriter* writer = new SeisWriter(_fs, _hdfs_data_name, _meta, _storage_seis, _data_name);
    if (!writer->Init()) {
        delete writer;
        return NULL;
    }
    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return writer;
}

/*
 * update only
 */
HeadUpdater* HDFS::OpenHeadUpdater() {
    bool is_auto_compact = false;
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    switch (_meta->head_placement) {
        case BY_ROW: {
            SeisHeadUpdaterRow* headUpdaterRow = new SeisHeadUpdaterRow(_fs, _hdfs_data_name, _meta,
                    is_auto_compact);
            if (!headUpdaterRow->Init()) {
                delete headUpdaterRow;
                return NULL;
            }
            TouchSeisModifyTime(_fs, _hdfs_data_name);
            return headUpdaterRow;
        }
            break;
        case BY_COLUMN: {
            SeisHeadUpdaterColumn* headUpdaterColumn = new SeisHeadUpdaterColumn(_fs,
                    _hdfs_data_name, _meta, is_auto_compact);
            if (!headUpdaterColumn->Init()) {
                delete headUpdaterColumn;
                return NULL;
            }
            TouchSeisModifyTime(_fs, _hdfs_data_name);
            return headUpdaterColumn;
        }
            break;
        default: {
            return NULL;
        }
            break;
    }

}

TraceUpdater* HDFS::OpenTraceUpdater() {
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return NULL;
    }
    bool is_auto_compact = false;
    SeisTraceUpdater* traceUpdater = new SeisTraceUpdater(_fs, _hdfs_data_name, _meta,
            is_auto_compact);
    if (!traceUpdater->Init()) {
        delete traceUpdater;
        return NULL;
    }
    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return traceUpdater;
}

bool HDFS::Remove() {
    if (!Exists()) {
        return true;
    }

    int rm_ret = hdfsDelete(_fs, _hdfs_data_name.c_str(), 1);
    if (rm_ret == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_RMDIR, errno);
        return false;
    }
    _meta = new MetaSeisHDFS();
    return true;
}

bool HDFS::Rename(const std::string& new_name) {

    if (new_name.length() == 0 || new_name[0] != '/') {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }

    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return false;
    }

    std::string new_full_data_name = _root_dir + new_name;
    std::string new_hdfs_data_name = hdfsGetPath(new_full_data_name);

    int iRet = hdfsRename(_fs, _hdfs_data_name.c_str(), new_hdfs_data_name.c_str());
    if (-1 == iRet) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_RENAME, errno);
        return false;
    }

    _data_name = new_name;
    _hdfs_data_name = new_hdfs_data_name;
    _hdfs_meta_name = _hdfs_data_name + "/" + "seis.meta";

    TouchSeisAccessTime(_fs, _hdfs_data_name);
    return true;
}

int HDFS::GetKeyNum(){
    if(_meta){
        return _meta->head_length;
    }
    return -1;
}

bool HDFS::Copy(const std::string& dest_dir) {
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return false;
    }

    if (!StartsWith(dest_dir, "/")) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_SUPPORT;
        return false;
    }

    if (!_storage_seis->PutMeta(_meta, _data_name)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
        return false;
    }

    Info("calling copy, from %s to %s\n", _data_name.c_str(), dest_dir.c_str());
    fflush(stdout);

    std::vector<std::string> paths = SplitString(_data_name, "/");
    std::string clean_dir_name = paths[paths.size() - 1];

    std::string full_dest_dir = _root_dir + dest_dir;
    std::string hdfs_dest_dir = hdfsGetPath(full_dest_dir);
    if (hdfs_dest_dir == "") {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_SUPPORT;
        return false;
    }
    if (hdfsExists(_fs, hdfs_dest_dir.c_str()) == 0) {

        std::string real_dest_path = dest_dir + "/" + clean_dir_name;
        Warn("%s exists, real_dest_path = %s\n", dest_dir.c_str(), real_dest_path.c_str());
        fflush(stdout);
        //RecursiveCopy

        std::string hdfs_real_dest_path = hdfsGetPath(_root_dir + real_dest_path);

        if (!RecursiveCopy(_hdfs_data_name, hdfs_real_dest_path)) {
            return false;
        }

    } else { // dest not exist

        std::string real_dest_path = dest_dir;

        Warn("%s does not exist, real_dest_path = %s\n", dest_dir.c_str(), real_dest_path.c_str());
        fflush(stdout);

        std::vector<std::string> dest_paths = SplitString(dest_dir, "/");
        std::string dest_parent_path = "";
        for (int i = 0; i < (int) (dest_paths.size() - 1); ++i) {
            dest_parent_path = dest_parent_path + "/" + dest_paths[i];
        }

        std::string hdfs_dest_parent_path = hdfsGetPath(_root_dir + dest_parent_path);
        if (hdfs_dest_parent_path == "") {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_SUPPORT;
            return false;
        }
        if (-1 == hdfsExists(_fs, hdfs_dest_parent_path.c_str())) {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
            return false;
        }

        std::string hdfs_real_dest_path = hdfsGetPath(_root_dir + real_dest_path);
        if (hdfs_real_dest_path == "") {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_SUPPORT;
            return false;
        }

        if (!RecursiveCopy(_hdfs_data_name, hdfs_real_dest_path)) {
            return false;
        }

    }

    TouchSeisAccessTime(_fs, _hdfs_data_name);
    return true;
}

bool HDFS::RecursiveCopy(const std::string &real_src_path,
        const std::string &real_dest_path) {

    Info("calling recursive copy, from %s to %s\n", real_src_path.c_str(), real_dest_path.c_str());
    fflush(stdout);

    if (-1 == hdfsCreateDirectory(_fs, real_dest_path.c_str())) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR, errno);
        return false;
    }

    hdfsFileInfo * hdfsDirInfo;
    int file_count;
    hdfsDirInfo = hdfsListDirectory(_fs, real_src_path.c_str(), &file_count);
    if (hdfsDirInfo == NULL) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_STAT, errno);
        return false;
    }

    if (file_count == 0) {
        hdfsFreeFileInfo(hdfsDirInfo, file_count);
        return true;
    } else {
        std::list<std::string> child_dir_list, child_file_list;
        for (int i = 0; i < file_count; i++) {
            std::string hdfs_file_name = hdfsDirInfo[i].mName;
            std::string sub_file_name = hdfs_file_name.substr(real_src_path.length());
            if (hdfsDirInfo[i].mKind == 'F') {
                child_file_list.push_back(sub_file_name);
            } else if (hdfsDirInfo[i].mKind == 'D') {
                child_dir_list.push_back(sub_file_name);
            }
        }

        for (std::list<std::string>::iterator it = child_file_list.begin();
                it != child_file_list.end(); ++it) {
            std::string src_child_file_path = real_src_path + "/" + *it;
            std::string dest_child_file_path = real_dest_path + "/" + *it;

            hdfsFileInfo * fileInfo = hdfsGetPathInfo(_fs, src_child_file_path.c_str());
            if (fileInfo == NULL) {
                hdfsFreeFileInfo(hdfsDirInfo, file_count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_STAT, errno);
                return false;
            }
            int64_t size = fileInfo->mSize;
            hdfsFreeFileInfo(fileInfo, 1);

            int buf_size;
            int64_t max_buf_size = 1;
            max_buf_size <<= 30; //1G
            if (size > max_buf_size) {
                buf_size = max_buf_size;
            } else {
                buf_size = size;
            }
            ScopedPointer<char> buf(new char[buf_size]);
            int64_t left_size = size;

            hdfsFile src_fd = hdfsOpenFile(_fs, src_child_file_path.c_str(),
            O_RDONLY, 0, REPLICATION, 0);
            if (src_fd == NULL) {
                hdfsFreeFileInfo(hdfsDirInfo, file_count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }

            hdfsFile dest_fd = hdfsOpenFile(_fs, dest_child_file_path.c_str(),
            O_WRONLY | O_TRUNC, 0, REPLICATION, 0);
            if (dest_fd == NULL) {
                hdfsCloseFile(_fs, src_fd);
                hdfsFreeFileInfo(hdfsDirInfo, file_count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }

            while (left_size > 0) {

                int read_size = left_size > buf_size ? buf_size : left_size;

                int64_t ret = SafeRead(_fs, src_fd, buf.data(), read_size);
                if (ret != read_size) {
                    hdfsFreeFileInfo(hdfsDirInfo, file_count);
                    hdfsCloseFile(_fs, src_fd);
                    hdfsCloseFile(_fs, dest_fd);
                    return false;
                }

                ret = SafeWrite(_fs, dest_fd, buf.data(), read_size);
                if (ret != read_size) {
                    hdfsFreeFileInfo(hdfsDirInfo, file_count);
                    hdfsCloseFile(_fs, src_fd);
                    hdfsCloseFile(_fs, dest_fd);
                    return false;
                }

                left_size -= read_size;
            }

            if (hdfsCloseFile(_fs, src_fd) == -1) {
                hdfsCloseFile(_fs, dest_fd);
                hdfsFreeFileInfo(hdfsDirInfo, file_count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }

            if (hdfsCloseFile(_fs, dest_fd) == -1) {
                hdfsFreeFileInfo(hdfsDirInfo, file_count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }

        }

        for (std::list<std::string>::iterator it = child_dir_list.begin();
                it != child_dir_list.end(); ++it) {
            std::string src_child_dir_path = real_src_path + "/" + *it;
            std::string dest_child_dir_path = real_dest_path + "/" + *it;

            if (!RecursiveCopy(src_child_dir_path, dest_child_dir_path)) {
                hdfsFreeFileInfo(hdfsDirInfo, file_count);
                return false;
            }
        }
        hdfsFreeFileInfo(hdfsDirInfo, file_count);
    }
    return true;

}

bool HDFS::SetLifetime(Lifetime lifetime_days) {
    if (!Exists()) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_EXIST;
        return false;
    }
    lifetime_days = lifetime_days > LIFETIME_DEFAULT ? LIFETIME_DEFAULT : lifetime_days; // not bigger than 1 month
    Lifetime old_lifetime_days = _meta->meta.lifetime;
    _meta->meta.lifetime = lifetime_days;
    if (!_storage_seis->PutMeta(_meta, _data_name)) {
        _meta->meta.lifetime = old_lifetime_days;
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
        return false;
    }
    TouchSeisAccessTime(_fs, _hdfs_data_name);
    return true;
}

/**
 * property
 */
bool HDFS::Exists() {
    return hdfsExists(_fs, _hdfs_data_name.c_str()) == -1 ? false : true;
}

int64_t HDFS::Size() {
    //return _meta->head_total_rows +  _meta->trace_total_rows;
    return _meta->head_total_rows * _meta->head_row_bytes
            + _meta->trace_total_rows * _meta->trace_row_bytes;
}

int HDFS::GetTraceSize() {
    return _meta->trace_row_bytes;
}

int HDFS::GetHeadSize() {
    return _meta->head_row_bytes;
}

Lifetime HDFS::GetLifetime() {
    return _meta->meta.lifetime;
}

time_t HDFS::LastModified() {
    int64_t modify_time_ms = 0;
    GetSeisTimeInfo(_fs, _hdfs_data_name, &modify_time_ms, NULL);
    return modify_time_ms;
}

time_t HDFS::LastAccessed() {
    int64_t access_time_ms = 0;
    GetSeisTimeInfo(_fs, _hdfs_data_name, NULL, &access_time_ms);
    return access_time_ms;
}

std::string HDFS::GetName() {
    return _data_name;
}

LifetimeStat HDFS::GetLifetimeStat() //?
{
    // need to check in local for this SeisFileHDFS, if it does exist return DELETED_NOT_EXPIRED
    if (!Exists()) {
        return INVALID;
    }

    int64_t lifetime_ms = (int64_t) _meta->meta.lifetime * 24 * 3600 * 1000;
    int64_t now_ms = GetCurrentMillis();
    int64_t last_access_ms = LastAccessed();
    if (now_ms - last_access_ms <= lifetime_ms) {
        return NORMAL;
    } else {
        return EXPIRED;
    }
}

void HDFS::SetLifetimeStat(LifetimeStat tag) {
    LifetimeStat old_tag = (_meta->meta).tag;
    (_meta->meta).tag = tag;
    if (!_storage_seis->PutMeta(_meta, _data_name)) {
        (_meta->meta).tag = old_tag;
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
    }
}

bool HDFS::Merge(const std::string& src_data_name) {
    SharedPtr<MetaSeisHDFS> src_meta = _storage_seis->GetMeta(src_data_name);
    if (src_meta == NULL || src_meta->head_placement != _meta->head_placement
            || src_meta->head_length != _meta->head_length
            || src_meta->trace_length != _meta->trace_length
            || src_meta->file_max_rows != _meta->file_max_rows) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_MERGE_FAULT, errno);
        return false;
    }
    if (src_meta->head_placement == BY_COLUMN && src_meta->column_turn != _meta->column_turn) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_MERGE_FAULT, errno);
        return false;
    }

    SeisMerger* seisMerger = new SeisMerger(_fs, _hdfs_data_name, _meta);
    if (!seisMerger->Merge(src_meta, _root_dir, src_data_name)) {
        delete seisMerger;
        return false;
    }
    delete seisMerger;

    std::string hdfs_src_data_name = hdfsGetPath(_root_dir + "/" + src_data_name);
    if (hdfsDelete(_fs, hdfs_src_data_name.c_str(), 1) == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE, errno);
        return false;
    }

    if (!_storage_seis->PutMeta(_meta, _data_name)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
        return false;
    }

    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return true;
}

bool HDFS::TruncateTrace(int64_t traces) {
    SeisTruncater* truncater = new SeisTruncater(_fs, _hdfs_data_name, _meta);
    if (!truncater->TruncateTrace(traces)) {
        delete truncater;
        return false;
    }
    delete truncater;

    if (!_storage_seis->PutMeta(_meta, _data_name)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
        return false;
    }

    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return true;
}

bool HDFS::TruncateHead(int64_t heads) {
    SeisTruncater* truncater = new SeisTruncater(_fs, _hdfs_data_name, _meta);
    if (!truncater->TruncateHead(heads)) {
        delete truncater;
        return false;
    }
    delete truncater;

    if (!_storage_seis->PutMeta(_meta, _data_name)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
        return false;
    }

    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return true;
}

bool HDFS::Compact() {

    switch (_meta->head_placement) {
        case BY_ROW: {
            SeisHeadCompactorRow* headCompactor = new SeisHeadCompactorRow(_fs, _hdfs_data_name,
                    _meta);
            if (!headCompactor->Init()) {
                delete headCompactor;
                return false;
            }
            if (!headCompactor->Compact()) {
                delete headCompactor;
                return false;
            }
            delete headCompactor;
        }
            break;
        case BY_COLUMN: {
            SeisHeadCompactorColumn* headCompactor = new SeisHeadCompactorColumn(_fs,
                    _hdfs_data_name, _meta);
            if (!headCompactor->Init()) {
                delete headCompactor;
                return false;
            }
            if (!headCompactor->Compact()) {
                delete headCompactor;
                return false;
            }
            delete headCompactor;
        }
            break;
        default: {
            return false;
        }
            break;
    }

    SeisTraceCompactor* traceCompactor = new SeisTraceCompactor(_fs, _hdfs_data_name, _meta);
    if (!traceCompactor->Init()) {
        delete traceCompactor;
        return false;
    }
    if (!traceCompactor->Compact()) {
        delete traceCompactor;
        return false;
    }
    delete traceCompactor;

    TouchSeisAccessTime(_fs, _hdfs_data_name);
    return true;
}

int64_t HDFS::GetUpdatedTraceRows() {
    int64_t sum = 0;
    for (int i = 0; i < _meta->trace_dir_size; i++) {
        std::vector<int64_t> index_vector;
        std::vector<pair<int64_t, int64_t>> trun_vector;
        std::string uuid_dir = _hdfs_data_name + "/" + _meta->trace_dir_uuid_array[i] + "/Update_Index";
        int count = 0;
        hdfsFileInfo * listFileInfo = hdfsListDirectory(_fs, uuid_dir.c_str(), &count);
        if (listFileInfo == NULL) {
            return -1;
        }
        for(int j=0;j<count;j++){
            if(listFileInfo[j].mKind==kObjectKindDirectory){
                sum+=GetTotalFileSize(_fs, listFileInfo[j].mName)/(2*sizeof(int64_t));
            }
        }
        hdfsFreeFileInfo(listFileInfo, count);
//        if (!GetIndexInfo(_fs, uuid_dir, index_vector)) //get all the index information, these index information is the index of updated trace
//                {
//            return -1;
//        }
//        if (!GetTruncateInfo(_fs, uuid_dir, trun_vector)) //get all the truncate information
//                {
//            return -1;
//        }
//        sum += GetUpdatedRow(index_vector, trun_vector);
    }
    return sum;
}

int64_t HDFS::GetUpdatedHeadRows() {

    int64_t sum = 0;
    for (int i = 0; i < _meta->head_dir_size; i++) {
        std::vector<int64_t> index_vector;
        std::vector<pair<int64_t, int64_t>> trun_vector;
        std::string uuid_dir = _hdfs_data_name + "/" + _meta->head_dir_uuid_array[i] + "/Update_Index";
        int count = 0;
        hdfsFileInfo * listFileInfo = hdfsListDirectory(_fs, uuid_dir.c_str(), &count);
        if (listFileInfo == NULL) {
            return -1;
        }
        for(int j=0;j<count;j++){
            if(listFileInfo[j].mKind==kObjectKindDirectory){
                sum+=GetTotalFileSize(_fs, listFileInfo[j].mName)/(2*sizeof(int64_t));
            }
        }
        hdfsFreeFileInfo(listFileInfo, count);
//        if (!GetIndexInfo(_fs, uuid_dir, index_vector)) //get all the index information, these index information is the index of updated head
//                {
//            return -1;
//        }
//        if (!GetTruncateInfo(_fs, uuid_dir, trun_vector)) //get all the truncate information
//                {
//            return -1;
//        }
//        sum += GetUpdatedRow(index_vector, trun_vector);
    }
    return sum;
}

uint32_t HDFS::GetTraceNum() {
    if (_meta->head_total_rows != _meta->trace_total_rows) {
        Warn(
                "The number of heads is not identical to the number of traces, return the small one as traceNum!");
    }
    return _meta->head_total_rows < _meta->trace_total_rows ?
            _meta->head_total_rows : _meta->trace_total_rows;
}

NAMESPACE_END_SEISFS
}
