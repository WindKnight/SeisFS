/*
 * utils.h
 *
 *  Created on: Jun 12, 2016
 *      Author: neo
 */

#ifndef GCACHE_SEIS_UTILS_H_
#define GCACHE_SEIS_UTILS_H_

#include <gcache_string.h>
#include <string>
#include <hdfs.h>
#include <vector>
#include <unordered_map>
#include <set>
#include <list>
#include <gcache_io.h>
#include "gcache_global.h"
#include "gcache_seis_helper.h"
#include "gcache_hdfs_utils.h"
#include "seisfs_row_filter.h"

using namespace std;
using namespace __gnu_cxx;

struct Slider {
    int v_id; //indicate which part of the vector
    int64_t v_in_id; //indicate which part of the scope
};

char* GetNewUuid();

int64_t BinarySearch(int64_t array[], int64_t low, int64_t high, int64_t target);

int64_t GetDirIndex(const int64_t* array, int64_t len, int64_t row_num);

int64_t GetTotalFileSize(hdfsFS fs, const string &dest_dir);

bool TruncateToNeatFile(hdfsFS fs, const string &dest_dir, int64_t row_size);

bool TruncateUpdateToNeatFile(hdfsFS fs, const string &src_dir, int64_t row_size);

int64_t GetTotalFileNumber(hdfsFS fs, const string &dest_dir);

void GetLastFileInfo(hdfsFS fs, const string &dir_name, string& file_name, int64_t& file_size);

string GetFormatCurrentMillis();

string GetUpdateFileName();

bool CreateAllDir(hdfsFS fs, const string &hdfs_data_name, const char* uuid);

bool GetIndexInfo(hdfsFS fs, const string &uuid_dir, vector<RichFileHandle>& upd_dat_fd_vector,
        vector<pair<int64_t, pair<int, int64_t>>>& index_vector, const int64_t & update_dir_num);

//bool GetIndexInfo(hdfsFS fs, const string &uuid_dir, vector<int64_t>& row_number_vector);

bool GetTruncateInfo(hdfsFS fs, const string &trace_dir,
        vector<pair<int64_t, int64_t>>& trun_vector);

void FilterTruncatedData(unordered_map<int64_t, pair<int, int64_t>>& idx_hash_map,
        vector<pair<int64_t, pair<int, int64_t>>>& index_vector,
        vector<pair<int64_t, int64_t>>& trun_vector);

void FilterTruncatedData(vector<vector<pair<int64_t, pair<int, int64_t>>>*>& index_vector_array,
        vector<pair<int64_t, pair<int, int64_t>>>& index_vector,
        vector<pair<int64_t, int64_t>>& trun_vector, int64_t file_max_rows);

bool OpenAllFile(hdfsFS fs, const string &dest_dir, vector<RichFileHandle>& fd_vector);

bool CloseAllFile(hdfsFS fs, vector<RichFileHandle>& fd_vector);

string GetRealFileName(const string& full_file_name);

int64_t GetArraySum(const int* array, int len);

int64_t* GetHeadOffset(const int* head_offset, int len);

bool GetNextSlider(const std::vector<seisfs::RowScope> &interval_list, Slider &slider);

bool GetSeekSlider(const std::vector<seisfs::RowScope> &interval_list, int64_t row,
        Slider &slider);

bool GetSeisTimeInfo(hdfsFS fs, const string& hdfs_seis_name, int64_t* modify_time_ms,
        int64_t* access_time_ms);

bool TouchSeisModifyTime(hdfsFS fs, const string& hdfs_seis_name);

bool TouchSeisAccessTime(hdfsFS fs, const string& hdfs_seis_name);

hdfsFile GetFileHandle(hdfsFS fs, vector<RichFileHandle>& rich_fd_vector,
        vector<int>& file_number_cache, int file_number);

int64_t GetUpdatedRow(vector<int64_t>& index_vector, vector<pair<int64_t, int64_t>>& trun_vector);
#endif /* GCACHE_SEIS_UTILS_H_ */
