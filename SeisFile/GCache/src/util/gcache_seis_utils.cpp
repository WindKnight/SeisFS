/*
 * utils.cpp
 *
 *  Created on: Jun 12, 2016
 *      Author: neo
 */

#include "gcache_seis_utils.h"
#include "uuid/uuid.h"
#include <algorithm>
#include "seisfs_row_filter.h"
#define UUID_LEN 37

char* GetNewUuid() {
    uuid_t id;
    uuid_generate(id);
    char * str = new char[UUID_LEN];
    uuid_unparse(id, str);
    return str;
}

int64_t BinarySearch(const int64_t* array, int64_t low, int64_t high, int64_t target) {

    while (low <= high) {
        int64_t mid = (low + high) / 2;
        if (array[mid] > target) high = mid - 1;
        else if (array[mid] < target) low = mid + 1;
        else return mid;
    }
    return high;
}

int64_t GetDirIndex(const int64_t* array, int64_t len, int64_t row_num) {
    return BinarySearch(array, 0, len - 1, row_num);
}

int64_t GetTotalFileSize(hdfsFS fs, const string &dest_dir) {
    int count = 0;
    hdfsFileInfo * listFileInfo = hdfsListDirectory(fs, dest_dir.c_str(), &count);
    if (listFileInfo == NULL) {
        return -1;
    }

    int64_t size_sum = 0;

    for (int i = 0; i < count; ++i) {
        size_sum += listFileInfo[i].mSize;
    }
    hdfsFreeFileInfo(listFileInfo, count);

    return size_sum;
}
/*
 * zhounan
 */
bool TruncateToNeatFile(hdfsFS fs, const string &dest_dir, int64_t row_size) {
    int count = 0;
    hdfsFileInfo * listFileInfo = hdfsListDirectory(fs, dest_dir.c_str(), &count);
    if (listFileInfo == NULL) {
        return false;
    }

    for (int i = 0; i < count; ++i) {

        int64_t truncateSize = listFileInfo[i].mSize / row_size * row_size;
        if (listFileInfo[i].mSize % row_size != 0) {
            if (truncateSize > 0) {
                if (!hdfsTruncateWait(fs, listFileInfo[i].mName, truncateSize) != 0) return false;
            } else {
                if (hdfsDelete(fs, listFileInfo[i].mName, 1) == -1) {
                    return false;
                }
            }
        }
    }
    hdfsFreeFileInfo(listFileInfo, count);

    return true;
}
/*
 * end
 */

/*
 *  zhounan
 */
bool TruncateUpdateToNeatFile(hdfsFS fs, const string &src_dir, int64_t row_size) {

    int data_count, idx_count = 0;
    string data_dir = src_dir + "/" + "Update_Data";
    string idx_dir = src_dir + "/" + "Update_Index";
    hdfsFileInfo * data_listFileInfo = hdfsListDirectory(fs, data_dir.c_str(), &data_count);
    hdfsFileInfo * idx_listFileInfo = hdfsListDirectory(fs, idx_dir.c_str(), &idx_count);
    if (data_listFileInfo == NULL || idx_listFileInfo == NULL) return false;
    if (data_count == 0 || idx_count == 0) return true;
    int i = 0;
    for (; i < idx_count && i < data_count; i++) {
        int64_t data_num = data_listFileInfo[i].mSize / row_size;
        int64_t data_offset = data_listFileInfo[i].mSize % row_size;
        int64_t idx_num = idx_listFileInfo[i].mSize / sizeof(int64_t);
        int64_t idx_offset = data_listFileInfo[i].mSize % sizeof(int64_t);
        if (data_num > idx_num) {
            if (-1 == hdfsTruncateWait(fs, data_listFileInfo[i].mName, idx_num * row_size)) {
                return false;
            }
            data_offset = 0;
        } else if (data_num < idx_num) {
            if (-1 == hdfsTruncateWait(fs, idx_listFileInfo[i].mName, data_num * sizeof(int64_t))) {
                return false;
            }
            idx_offset = 0;
        } else if (data_offset != 0) {
            if (-1 == hdfsTruncateWait(fs, data_listFileInfo[i].mName, data_num * row_size)) {
                return false;
            }
        } else if (idx_offset != 0) {
            if (-1 == hdfsTruncateWait(fs, idx_listFileInfo[i].mName, idx_num * sizeof(int64_t))) {
                return false;
            }
        }
    }

    if (idx_count != data_count) {
        if (i == idx_count) {
            for (; i < data_count; i++) {
                if (-1 == hdfsDelete(fs, data_listFileInfo[i].mName, 0)) {
                    return false;
                }
            }
        } else if (i == data_count) {
            for (; i < idx_count; i++) {
                if (-1 == hdfsDelete(fs, idx_listFileInfo[i].mName, 0)) {
                    return false;
                }
            }
        }
    }
    hdfsFreeFileInfo(data_listFileInfo, data_count);
    hdfsFreeFileInfo(idx_listFileInfo, idx_count);

    return true;
}
/*
 * end
 */

int64_t GetTotalFileNumber(hdfsFS fs, const string &dest_dir) {
    int count = 0;
    hdfsFileInfo * listFileInfo = hdfsListDirectory(fs, dest_dir.c_str(), &count);
    if (listFileInfo == NULL) {
        return -1;
    }
    hdfsFreeFileInfo(listFileInfo, count);
    return count;
}

void GetLastFileInfo(hdfsFS fs, const string &dir_name, string& file_name, int64_t& file_size) {
    int count = 0;
    hdfsFileInfo * listFileInfo = hdfsListDirectory(fs, dir_name.c_str(), &count);
    if (listFileInfo == NULL) {
        file_name = "-1";
        return;
    }

    if (count == 0) {
        file_name = "0";
        return;
    }

    file_name = listFileInfo[count - 1].mName;
    file_size = listFileInfo[count - 1].mSize;
    hdfsFreeFileInfo(listFileInfo, count);

}

string GetFormatCurrentMillis() {
    char time_c[19];
    int64_t time = GetCurrentMillis();
    sprintf(time_c, "%018ld", time);
    string time_str = time_c;
    return time_str;
}

string GetUpdateFileName() {

    uuid_t file_uuid;
    uuid_generate(file_uuid);
    char file_uuid_c[37];
    uuid_unparse(file_uuid, file_uuid_c);
    file_uuid_c[36] = 0;
    string file_uuid_str = file_uuid_c;
    string time_str = GetFormatCurrentMillis();
    string file_name = time_str + "_" + file_uuid_str;
    return file_name;
}

bool CreateAllDir(hdfsFS fs, const string &hdfs_data_name, const char* uuid) {

    string seis_uuid_dir = hdfs_data_name + "/" + uuid;
    string seis_upd_idx_dir = seis_uuid_dir + "/" + "Update_Index";
    string seis_upd_dat_dir = seis_uuid_dir + "/" + "Update_Data";
    string seis_wrt_dat_dir = seis_uuid_dir + "/" + "Write_Data";
    string seis_tru_dat_dir = seis_uuid_dir + "/" + "Truncate_Data";

    if (-1 == hdfsCreateDirectory(fs, hdfs_data_name.c_str())
            || -1 == hdfsCreateDirectory(fs, seis_uuid_dir.c_str())
            || -1 == hdfsCreateDirectory(fs, seis_upd_idx_dir.c_str())
            || -1 == hdfsCreateDirectory(fs, seis_upd_dat_dir.c_str())
            || -1 == hdfsCreateDirectory(fs, seis_wrt_dat_dir.c_str())
            || -1 == hdfsCreateDirectory(fs, seis_tru_dat_dir.c_str())) {
        return false;
    }

    return true;
}

bool GetIndexInfo(hdfsFS fs, const string &uuid_dir, vector<RichFileHandle>& upd_dat_fd_vector,
        vector<pair<int64_t, pair<int, int64_t>>>& index_vector, const int64_t & update_dir_num) {
    string idx_dir_name = uuid_dir + "/Update_Index/No_" + Int642Str(update_dir_num);
    string dat_dir_name = uuid_dir + "/Update_Data/No_" + Int642Str(update_dir_num);

    if (hdfsExists(fs, idx_dir_name.c_str()) == -1 || hdfsExists(fs, dat_dir_name.c_str()) == -1) {
        return true;
    }

    int idx_file_count = 0;
    hdfsFileInfo * idxInfoList = hdfsListDirectory(fs, idx_dir_name.c_str(), &idx_file_count);
    if (idxInfoList == NULL) {
        //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    }

    int dat_file_count = 0;
    hdfsFileInfo * datInfoList = hdfsListDirectory(fs, dat_dir_name.c_str(), &dat_file_count);
    if (datInfoList == NULL) {
        //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        hdfsFreeFileInfo(idxInfoList, idx_file_count);
        return false;
    }

    for (int i = 0; i < idx_file_count; ++i) {
        hdfsFile idx_fd = hdfsOpenFile(fs, idxInfoList[i].mName, O_RDONLY, 0,
        REPLICATION, 0);
        if (idx_fd == NULL) {
            hdfsFreeFileInfo(datInfoList, dat_file_count);
            hdfsFreeFileInfo(idxInfoList, idx_file_count);
            //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE,errno);
            return false;
        }

        int64_t read_len = idxInfoList[i].mSize;
        char *readData = new char[read_len];
        int64_t ret = SafeRead(fs, idx_fd, readData, read_len);
        if (ret != read_len) {
            delete[] readData;
            hdfsCloseFile(fs, idx_fd);
            hdfsFreeFileInfo(datInfoList, dat_file_count);
            hdfsFreeFileInfo(idxInfoList, idx_file_count);
            //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return false;
        }

        int64_t *idxDat = (int64_t*) readData;
        int64_t items_count = read_len / (int64_t) sizeof(int64_t);
        for (int64_t j = 0; j < items_count; j++) {
            index_vector.push_back(
                    pair<int64_t, pair<int, int64_t>>(idxDat[j],
                            pair<int, int64_t>(upd_dat_fd_vector.size(), j)));
        }

        RichFileHandle rich_fd;
        rich_fd.fd = NULL;
        rich_fd.file_name = datInfoList[i].mName;
        rich_fd.access_time = GetCurrentMillis();
        upd_dat_fd_vector.push_back(rich_fd);

        delete[] readData;
        hdfsCloseFile(fs, idx_fd);
    }

    hdfsFreeFileInfo(datInfoList, dat_file_count);
    hdfsFreeFileInfo(idxInfoList, idx_file_count);
    return true;
}

//bool GetIndexInfo(hdfsFS fs, const string &uuid_dir, vector<int64_t>& row_number_vector) {
//
//    string idx_dir_name = uuid_dir + "/Update_Index";
//
//    int idx_file_count = 0;
//    hdfsFileInfo * idxInfoList = hdfsListDirectory(fs, idx_dir_name.c_str(), &idx_file_count);
//    if (idxInfoList == NULL) {
//        //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
//        return false;
//    }
//
//    for (int i = 0; i < idx_file_count; ++i) {
//        hdfsFile idx_fd = hdfsOpenFile(fs, idxInfoList[i].mName, O_RDONLY, 0,
//        REPLICATION, 0);
//        if (idx_fd == NULL) {
//            hdfsFreeFileInfo(idxInfoList, idx_file_count);
//            //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE,errno);
//            return false;
//        }
//
//        int64_t read_len = idxInfoList[i].mSize;
//        char *readData = new char[read_len];
//        int64_t ret = SafeRead(fs, idx_fd, readData, read_len);
//        if (ret != read_len) {
//            delete[] readData;
//            hdfsCloseFile(fs, idx_fd);
//            hdfsFreeFileInfo(idxInfoList, idx_file_count);
//            //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
//            return false;
//        }
//
//        int64_t *idxDat = (int64_t*) readData;
//        int64_t items_count = read_len / (int64_t) sizeof(int64_t);
//        for (int64_t j = 0; j < items_count; j++) {
//            row_number_vector.push_back(idxDat[j]);
//        }
//
//        delete[] readData;
//        hdfsCloseFile(fs, idx_fd);
//    }
//
//    hdfsFreeFileInfo(idxInfoList, idx_file_count);
//    return true;
//}

bool GetTruncateInfo(hdfsFS fs, const string &trace_dir,
        vector<pair<int64_t, int64_t>>& trun_vector) {

    int count;
    string hdfs_idx_dir = trace_dir + "/" + "Update_Index";
    hdfsFileInfo * listFileInfo = hdfsListDirectory(fs, hdfs_idx_dir.c_str(), &count);
    if (listFileInfo == NULL) {
        //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    }

    if (count == 0) {
        return true;
    }

    string hdfs_file_name = trace_dir + "/" + "Truncate_Data" + "/Data";
    if (hdfsExists(fs, hdfs_file_name.c_str()) == -1) {
        hdfsFreeFileInfo(listFileInfo, count);
        return true;
    }

    hdfsFileInfo * fileInfo = hdfsGetPathInfo(fs, hdfs_file_name.c_str());
    if (fileInfo == NULL) {
        //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        hdfsFreeFileInfo(listFileInfo, count);
        return false;
    }
    int64_t item_count = fileInfo->mSize / TRUN_ROW_SIZE;
    hdfsFreeFileInfo(fileInfo, 1);

    hdfsFile fd = hdfsOpenFile(fs, hdfs_file_name.c_str(), O_RDONLY, 0,
    REPLICATION, 0);
    if (fd == NULL) {
        //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }

    int k = 0;
    int64_t file_size_sum = 0;
    char trunDat[TRUN_ROW_SIZE];
    for (int64_t i = 0; i < item_count; i++) {
        if (TRUN_ROW_SIZE != hdfsRead(fs, fd, trunDat, TRUN_ROW_SIZE)) {
            //errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            hdfsFreeFileInfo(listFileInfo, count);
            hdfsCloseFile(fs, fd);
            return false;
        }
        string info_item = trunDat;
        vector<std::string> split_item = SplitString(info_item, '|');
        string format_time = split_item[0];
        int64_t trun_row_idx = ToInt64(split_item[1]);

        while (k < count && GetRealFileName(listFileInfo[k].mName) < format_time) {
            file_size_sum += listFileInfo[k].mSize;
            k++;
        }

        int64_t rows = file_size_sum / sizeof(int64_t);

        while (trun_vector.size() > 0 && trun_row_idx <= trun_vector.front().first) {
            trun_vector.erase(trun_vector.begin());
        }
        trun_vector.push_back(pair<int64_t, int64_t>(trun_row_idx, rows));
    }

    hdfsFreeFileInfo(listFileInfo, count);
    hdfsCloseFile(fs, fd);
    return true;
}

void FilterTruncatedData(unordered_map<int64_t, pair<int, int64_t>>& idx_hash_map,
        vector<pair<int64_t, pair<int, int64_t>>>& index_vector,
        vector<pair<int64_t, int64_t>>& trun_vector) {
    if (trun_vector.size() == 0) {
        for (uint64_t i = 0; i < index_vector.size(); i++) {
            idx_hash_map[index_vector[i].first] = index_vector[i].second;
        }
    } else {

        int64_t pre_pos = 0;
        for (uint64_t i = 0; i < trun_vector.size(); i++) {
            for (int64_t j = pre_pos; j < trun_vector[i].second; j++) {
                if (index_vector[j].first < trun_vector[i].first) {
                    idx_hash_map[index_vector[j].first] = index_vector[j].second;
                }
            }
            pre_pos = trun_vector[i].second;
        }
        for (uint64_t j = pre_pos; j < index_vector.size(); j++) {
            idx_hash_map[index_vector[j].first] = index_vector[j].second;
        }
    }
}

void FilterTruncatedData(vector<vector<pair<int64_t, pair<int, int64_t>>>*>& index_vector_array,
        vector<pair<int64_t, pair<int, int64_t>>>& index_vector,
        vector<pair<int64_t, int64_t>>& trun_vector, int64_t file_max_rows) {
    if (trun_vector.size() == 0) {
        for (uint64_t i = 0; i < index_vector.size(); i++) {
            int64_t array_pos = index_vector[i].first / file_max_rows;
            if (index_vector_array[array_pos] == NULL) {
                index_vector_array[array_pos] = new vector<pair<int64_t, pair<int, int64_t>>>();
            }
            index_vector_array[array_pos]->push_back(index_vector[i]);
        }
    } else {

        int64_t pre_pos = 0;
        for (uint64_t i = 0; i < trun_vector.size(); i++) {

            for (int64_t j = pre_pos; j < trun_vector[i].second; j++) {
                if (index_vector[j].first < trun_vector[i].first) {
                    int64_t array_pos = index_vector[j].first / file_max_rows;
                    if (index_vector_array[array_pos] == NULL) {
                        index_vector_array[array_pos] =
                                new vector<pair<int64_t, pair<int, int64_t>>>();
                    }
                    index_vector_array[array_pos]->push_back(index_vector[j]);
                }
            }
            pre_pos = trun_vector[i].second;
        }
        for (uint64_t j = pre_pos; j < index_vector.size(); j++) {
            int64_t array_pos = index_vector[j].first / file_max_rows;
            if (index_vector_array[array_pos] == NULL) {
                index_vector_array[array_pos] = new vector<pair<int64_t, pair<int, int64_t>>>();
            }
            index_vector_array[array_pos]->push_back(index_vector[j]);
        }
    }
}

int64_t GetUpdatedRow(vector<int64_t>& index_vector, vector<pair<int64_t, int64_t>>& trun_vector) {

    vector<int64_t> row_number_vector;
    if (trun_vector.size() == 0) {
        sort(index_vector.begin(), index_vector.end());
        index_vector.erase(unique(index_vector.begin(), index_vector.end()), index_vector.end());
        return index_vector.size();
    } else {
        int64_t pre_pos = 0;
        for (uint64_t i = 0; i < trun_vector.size(); i++) {

            for (int64_t j = pre_pos; j < trun_vector[i].second; j++) {
                if (index_vector[j] < trun_vector[i].first) {
                    row_number_vector.push_back(index_vector[j]);
                }
            }
            pre_pos = trun_vector[i].second;
        }
        for (uint64_t j = pre_pos; j < index_vector.size(); j++) {
            row_number_vector.push_back(index_vector[j]);
        }

        sort(row_number_vector.begin(), row_number_vector.end());
        row_number_vector.erase(unique(row_number_vector.begin(), row_number_vector.end()),
                row_number_vector.end());
        return row_number_vector.size();
    }

}

bool OpenAllFile(hdfsFS fs, const string &dest_dir, vector<RichFileHandle>& fd_vector) {
    int count;
    hdfsFileInfo * listFileInfo = hdfsListDirectory(fs, dest_dir.c_str(), &count);
    if (listFileInfo == NULL) {
        return false;
    }

    for (int i = 0; i < count; ++i) {
        RichFileHandle rich_fd;
        rich_fd.fd = NULL;
        rich_fd.access_time = GetCurrentMillis();
        rich_fd.file_name = listFileInfo[i].mName;
        fd_vector.push_back(rich_fd);
    }
    hdfsFreeFileInfo(listFileInfo, count);
    return true;
}

bool CloseAllFile(hdfsFS fs, vector<RichFileHandle>& fd_vector) {
    for (vector<RichFileHandle>::iterator j = fd_vector.begin(); j != fd_vector.end(); j++) {
        if (j->fd && -1 == hdfsCloseFile(fs, j->fd)) {
            return false;
        }
    }
    return true;
}

string GetRealFileName(const string& full_file_name) {
    int pos = full_file_name.find_last_of('/');
    string real_file_name = full_file_name.substr(pos + 1, full_file_name.length() - pos - 1);
    return real_file_name;
}

int64_t GetArraySum(const int* array, int len) {
    int64_t sum = 0;
    for (int i = 0; i < len; ++i) {
        sum += array[i];
    }
    return sum;
}

int64_t* GetHeadOffset(const int* head_sizes, int len) {
    int64_t* head_offset = new int64_t[len];
    head_offset[0] = 0;
    for (int64_t i = 1; i < len; i++) {
        head_offset[i] = head_offset[i - 1] + head_sizes[i - 1];
    }
    return head_offset;
}

bool GetNextSlider(const std::vector<seisfs::RowScope> &interval_list, Slider &slider) {
    if (slider.v_id == -1) {
        slider.v_id = 0;
        slider.v_in_id = interval_list[slider.v_id].GetStartTrace();
        return true;
    }
    if (slider.v_in_id == interval_list[slider.v_id].GetStopTrace()) {
        if (slider.v_id + 1 >= (int) interval_list.size()) {
            slider.v_in_id = -1;
            return false;
        } else {
            slider.v_id++;
            slider.v_in_id = interval_list[slider.v_id].GetStartTrace();
        }
    } else {
        if (interval_list[slider.v_id].GetStartTrace()
                > interval_list[slider.v_id].GetStopTrace()) {
            slider.v_in_id--;
        } else {
            slider.v_in_id++;
        }
    }
    return true;
}

bool GetSeekSlider(const std::vector<seisfs::RowScope> &interval_list, int64_t row_idx,
        Slider &slider) {
    slider.v_id = -1;
    slider.v_in_id = -1;
    while (row_idx >= 0) {
        if (!GetNextSlider(interval_list, slider)) {
            return false;
        }
        row_idx--;
    }

    return true;
}

bool GetSeisTimeInfo(hdfsFS fs, const string& hdfs_seis_name, int64_t* modify_time_ms,
        int64_t* access_time_ms) {
    hdfsFileInfo * fileInfo = hdfsGetPathInfo(fs, hdfs_seis_name.c_str());
    if (fileInfo == NULL) {
        return false;
    }
    if (modify_time_ms) {
        *modify_time_ms = fileInfo->mLastMod * 1000;
    }
    if (access_time_ms) {
        *access_time_ms = fileInfo->mLastAccess * 1000;
    }
    hdfsFreeFileInfo(fileInfo, 1);
    return true;
}

bool TouchSeisModifyTime(hdfsFS fs, const string& hdfs_seis_name) {
    int64_t now_ms = GetCurrentMillis();
    return hdfsUtime(fs, hdfs_seis_name.c_str(), now_ms, now_ms) == -1 ? false : true;
}

bool TouchSeisAccessTime(hdfsFS fs, const string& hdfs_seis_name) {
    int64_t now_ms = GetCurrentMillis();
    return hdfsUtime(fs, hdfs_seis_name.c_str(), -1, now_ms) == -1 ? false : true;
}

hdfsFile GetFileHandle(hdfsFS fs, vector<RichFileHandle>& rich_fd_vector,
        vector<int>& file_number_cache, int file_number) {
    if (rich_fd_vector[file_number].fd) {
        rich_fd_vector[file_number].access_time = GetCurrentMillis();
        return rich_fd_vector[file_number].fd;
    }

    hdfsFile fd = hdfsOpenFile(fs, rich_fd_vector[file_number].file_name.c_str(), O_RDONLY, 0,
    REPLICATION, 0);
    if (fd == NULL) {
        return NULL;
    }

    rich_fd_vector[file_number].fd = fd;
    rich_fd_vector[file_number].access_time = GetCurrentMillis();
    file_number_cache.push_back(file_number);

    if (file_number_cache.size() > CACHE_FD_SIZE) {
        //LRU
        int64_t min_time = rich_fd_vector[*file_number_cache.begin()].access_time;
        vector<int>::iterator lru_file_number = file_number_cache.begin();
        for (vector<int>::iterator i = file_number_cache.begin() + 1; i != file_number_cache.end();
                ++i) {
            if (rich_fd_vector[*i].access_time < min_time) {
                min_time = rich_fd_vector[*i].access_time;
                lru_file_number = i;
            }
        }

        if (hdfsCloseFile(fs, rich_fd_vector[*lru_file_number].fd) == -1) {
            return NULL;
        }

        rich_fd_vector[*lru_file_number].fd = NULL;
        file_number_cache.erase(lru_file_number);
    }
    return fd;
}

