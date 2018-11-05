/*
 * gcache_seis_meta.h
 *
 *  Created on: Jul 7, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_META_H_
#define GCACHE_SEIS_META_H_

#include "gcache_global.h"
#include "seisfile_meta.h"
#include <stdint.h>
#include <stddef.h>
#include <vector>
#include <uuid/uuid.h>
#include <stdio.h>

NAMESPACE_BEGIN_SEISFS
namespace file {
//#define _COLUMN_SIZE 512000  //maximum turn
//#define _CACHE_ROWS 1000000
#define LIFETIME_DEFAULT 	30
#define COLUMN_TURN 10000  //for column to turn
#define FILE_MAX_ROWS_TIMES 1000 //times to column_turn for block size of file
#define UPDATE_BLOCK_ROW_TIMES 10 //times of file_max_row for update separately stored
#define DEFAULT_PLACEMENT  BY_ROW
#define HASH_MAP_MAX_SIZE UPDATE_BLOCK_ROW_TIMES*FILE_MAX_ROWS_TIMES*COLUMN_TURN/2
//#define FILE_MAX_ROWS_DEFAULT   50000000

struct MetaSeisHDFS {
    struct MetaHeader meta;
    int version;
    uint32_t head_length;
    HeadPlacementStruct head_placement;
    int64_t column_turn;
    uint32_t trace_length;
    time_t creation_time;

    int64_t file_max_rows;

    int64_t trace_type_size;

    //for update to separately store update data
    int64_t update_block_size;
    int64_t update_block_num;

//    bool move_to_nas;           //indicate copy to nas

//---------ice
    int64_t trace_total_rows;   // the total number of trace rows for this seis
    int64_t trace_dir_size; // the number of trace directories for this seis, when merge it will become larger
    int64_t trace_row_bytes;    // the number of bytes for a trace row

    int64_t head_total_rows;    // the total number of head rows for this seis
    int64_t head_dir_size; //the number of head directories for this seis, when merge it will become larger
    int64_t head_row_bytes;     // the number of bytes for a head row
    //---------

    int* head_sizes; // an array that indicates the number of bytes for each head field
    //---------ice
    char** trace_dir_uuid_array; // an array record each trace directory uuid name
    int64_t* trace_dir_rows_array; // an array record the number of trace rows for each trace directory
    int64_t* trace_dir_front_array; // an array record the order of directory front trace row

    char** head_dir_uuid_array; // an array record each head directory uuid name
    int64_t* head_dir_rows_array; // an array record the number of head rows for each head directory
    int64_t* head_dir_front_array; // an array record the order of directory front head row
    //---------

    MetaSeisHDFS() {
        meta.lifetime = 0;
        meta.tag = NORMAL;
        version = 0;
        head_length = 0;
        head_placement = BY_ROW;
        column_turn = -1;
        trace_length = 0;
        creation_time = 0;

        file_max_rows = -1;

        update_block_size = -1;
        update_block_num = -1;

        trace_type_size = (sizeof(float));

//        //indicate copy to nas
//        move_to_nas = false;

        //-------------ice
        trace_total_rows = 0;
        trace_dir_size = 0;
        trace_row_bytes = 0;
        head_total_rows = 0;
        head_dir_size = 0;
        head_row_bytes = 0;
        //-----------------

        head_sizes = NULL;

        //-------------ice
        trace_dir_uuid_array = NULL;
        trace_dir_rows_array = NULL;
        trace_dir_front_array = NULL;
        head_dir_uuid_array = NULL;
        head_dir_rows_array = NULL;
        head_dir_front_array = NULL;
        //-----------------
    }

    ~MetaSeisHDFS() {
        delete[] head_sizes;
        //----------------ice
        delete[] trace_dir_rows_array;
        delete[] trace_dir_front_array;

        if (trace_dir_uuid_array != NULL) {
            for (int64_t i = 0; i < trace_dir_size; i++) {
                delete[] trace_dir_uuid_array[i];
            }
        }
        delete[] trace_dir_uuid_array;

        delete[] head_dir_rows_array;
        delete[] head_dir_front_array;

        if (head_dir_uuid_array != NULL) {
            for (int64_t i = 0; i < head_dir_size; i++) {
                delete[] head_dir_uuid_array[i];
            }
        }
        delete[] head_dir_uuid_array;
        //----------------

    }
};

struct HeadTypeHDFS: HeadType {

    HeadPlacementStruct placement;

    HeadTypeHDFS() {
        placement = BY_ROW;
    }
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_META_H_ */
