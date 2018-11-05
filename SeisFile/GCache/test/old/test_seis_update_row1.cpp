/*
 * test_seis_column.cpp
 *
 *  Created on: 2017年5月2日
 *      Author: root
 */
#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include "gcache_seis_data.h"
#include "gcache_seis_meta.h"
#include "seisfile_headreader.h"
#include "seisfile_headwriter.h"
#include "gcache_seis_headreader_row.h"
#include "gcache_seis_headreader_column.h"
#include <sys/types.h>
#include <unistd.h>
#include "gcache_scoped_pointer.h"
#include "gcache_string.h"
#include <ctime>
#include <iostream>
#include <fstream>
#include <thread>
#include <math.h>

using namespace seisfs::file;
using namespace seisfs;
using namespace std;
using namespace boost;

BOOST_AUTO_TEST_SUITE(seis_test)

#define THRESHOLD 2147483647
#define GET_HOST_NUM 20

int64_t WRITE_DATA_SIZE;          //写入的总数据量    单位GB
int64_t FILE_PART_SIZE;           //文件分块大小      单位GB
int64_t TYPE_SIZE;                //关键字大小        单位B
int64_t TYPE_LENGTH;              //关键字个数
int64_t TOTAL_WRITE_ROWS;         //写入的行数
//int64_t FILE_MAX_ROWS;            //一个文件的最大行数
int64_t TOTAL_ROW_SIZE;           //一行大小         单位B
int64_t INTERVAL;                 //读取、更新间隔行数
int64_t TOTAL_ROW_NUM;            //当前seis总行数
//int64_t UPDATE_BLOCK_SIZE;
//
//int64_t _COLUMN_TURN_SIZE;

HeadPlacementStruct PLACEMENT;

string message_file_name = "./log_result";     //放置打印信息的文件名

struct updateArgv {                //更新函数的传入参数
    string seis_name;
    int update_time;              //更新的次数，与读取检测相关
    int start_num;                //开始更新或读取的行号
    bool need_host_name;
};

void SetTestParameters(int64_t write_data_size, int64_t file_part_size, int64_t type_size,
        int64_t type_length, int64_t interval, HeadPlacementStruct placement = BY_ROW) {
    WRITE_DATA_SIZE = write_data_size;
    FILE_PART_SIZE = file_part_size;
    TYPE_SIZE = type_size;
    TYPE_LENGTH = type_length;
    TOTAL_ROW_SIZE = type_size * type_length;
    TOTAL_WRITE_ROWS = (WRITE_DATA_SIZE * 1024 * 1024 * 1024 / (TYPE_SIZE * TYPE_LENGTH));
    //for test only
//    TOTAL_WRITE_ROWS = 31;
//    FILE_MAX_ROWS = (FILE_PART_SIZE * 1024 * 1024 * 1024 / (TYPE_SIZE * TYPE_LENGTH));
//    FILE_MAX_ROWS = 20;
//    UPDATE_BLOCK_SIZE = 4 * 40;
//    _COLUMN_TURN_SIZE = 512 * 10;

    INTERVAL = interval;
    PLACEMENT = placement;
    TOTAL_ROW_NUM = 0;
}

void PrintMessage(string message) {
    cout << message << endl;
    ofstream out(message_file_name.c_str(), ios::app);
    out << message << endl;
    out.close();
}

string GetFileName() {
    string filename = __FILE__;
    vector<string> filename_split = SplitString(filename, "/");
    filename_split = SplitString(filename_split[filename_split.size() - 1], ".");
    return filename_split[0];
}

bool WriteSeisHead(string seis_name) {
    SeisFile* seis = HDFS::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    HeadType headType;
    for (int i = 0; i < TYPE_LENGTH; i++)
        headType.head_size.push_back(TYPE_SIZE);
    HeadWriter* writer = seis->OpenHeadWriter(headType, 20);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    PrintMessage("start to write \"" + seis_name + "\" head");
    int head[headType.head_size.size()];
    int64_t start = 0, time_cost = 0;
    int64_t writer_pos = writer->Pos();
    for (int64_t i = writer_pos; i < writer_pos + TOTAL_WRITE_ROWS; ++i) {
        for (uint j = 0; j < headType.head_size.size(); j++) {
            head[j] = i % THRESHOLD;
        }
        start = GetCurrentMillis();
        BOOST_REQUIRE_MESSAGE(writer->Write(head) == true,
                "critical check writer->Write(head) == true has failed, i = " + Int642Str(i));
        time_cost = time_cost + GetCurrentMillis() - start;
    }
    start = GetCurrentMillis();
    writer->Sync();
    time_cost = time_cost + GetCurrentMillis() - start;
    writer_pos = writer->Pos();
    TOTAL_ROW_NUM += TOTAL_WRITE_ROWS;
    BOOST_REQUIRE_EQUAL(writer_pos, TOTAL_ROW_NUM);
    PrintMessage("finish writing \"" + seis_name + "\" head");
    PrintMessage("total time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average write speed: "
                    + Int642Str(TOTAL_WRITE_ROWS * TOTAL_ROW_SIZE / time_cost / (int64_t) 1024)
                    + "MB/s");
    delete writer;
    delete seis;
    return true;
}

bool WriteSeisTrace(string seis_name) {
    SeisFile* seis = HDFS::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    TraceType traceType;
    traceType.trace_size = TYPE_LENGTH;
    TraceWriter* writer = seis->OpenTraceWriter(traceType, 20);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    PrintMessage("start to write \"" + seis_name + "\" trace");
    int head[traceType.trace_size];
    int64_t start = 0, time_cost = 0;
    int64_t writer_pos = writer->Pos();
    for (int64_t i = writer_pos; i < writer_pos + TOTAL_WRITE_ROWS; ++i) {
        for (uint j = 0; j < traceType.trace_size; j++) {
            head[j] = i % THRESHOLD;
        }
        start = GetCurrentMillis();
        BOOST_REQUIRE_MESSAGE(writer->Write(head) == true,
                "critical check writer->Write(head) == true has failed, i = " + Int642Str(i));
        time_cost = time_cost + GetCurrentMillis() - start;
    }
    start = GetCurrentMillis();
    writer->Sync();
    time_cost = time_cost + GetCurrentMillis() - start;
    writer_pos = writer->Pos();
    TOTAL_ROW_NUM += TOTAL_WRITE_ROWS;
    BOOST_REQUIRE_EQUAL(writer_pos, TOTAL_ROW_NUM);
    PrintMessage("finish writing \"" + seis_name + "\" trace");
    PrintMessage("total time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average write speed: "
                    + Int642Str(TOTAL_WRITE_ROWS * TOTAL_ROW_SIZE / time_cost / (int64_t) 1024)
                    + "MB/s");
    delete writer;
    delete seis;
    return true;
}

bool SkipReadSeisHead(updateArgv* update_argv) {
    BOOST_REQUIRE_MESSAGE(update_argv!=NULL, "update_argv is null");
    SeisFile* seis = HDFS::New("/" + update_argv->seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    HeadReader* reader = seis->OpenHeadReader();
    BOOST_REQUIRE_MESSAGE(reader!=NULL, "reader is null");
    int readDat[TYPE_LENGTH];
    int64_t start = 0, read_size = 0, time_cost = 0;
    PrintMessage(
            "start to read \"" + update_argv->seis_name
                    + "\" head, using \"reader->Get(k, readDat)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_argv->update_time));
    PrintMessage("read checking, start with k=" + Int642Str(update_argv->start_num));
    for (int64_t k = update_argv->start_num; k < TOTAL_ROW_NUM; k += INTERVAL) {
        start = GetCurrentMillis();
        bool read_rst = reader->Get(k, readDat);
        time_cost = time_cost + GetCurrentMillis() - start;
        BOOST_REQUIRE_MESSAGE(read_rst == true, "read_rst is false, k = " + Int642Str(k));
        int64_t t = ((k + update_argv->update_time) % THRESHOLD);
        BOOST_REQUIRE_MESSAGE(readDat[0] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
        BOOST_REQUIRE_MESSAGE(readDat[TYPE_LENGTH - 1] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
        BOOST_REQUIRE_MESSAGE(readDat[TYPE_LENGTH / 2] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
        read_size += TOTAL_ROW_SIZE;
    }
    if (update_argv->need_host_name) {
        std::vector<int64_t> head_id;
        std::vector<std::vector<std::string>> hostInfos;
        for (int i = 0; i < GET_HOST_NUM; i++) {
            head_id.push_back(i);
        }
        if (DEFAULT_PLACEMENT == BY_ROW) {
            (dynamic_cast<SeisHeadReaderRow*>(reader))->GetLocations(head_id, hostInfos);
        } else {
            (dynamic_cast<SeisHeadReaderColumn*>(reader))->GetLocations(head_id, hostInfos);
        }

        for (int i = 0; i < GET_HOST_NUM; i++) {
            PrintMessage("hostname for row NO." + Int642Str(i));
            int j = 0;
            for (std::vector<std::string>::iterator it = hostInfos[i].begin();
                    it != hostInfos[i].end(); it++, j++) {
                PrintMessage("    [" + Int642Str(j) + "] " + *it);
            }
        }
    }
    delete reader;

    PrintMessage("read head checking finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average read speed: " + Int642Str(read_size / time_cost / (int64_t) 1024) + "MB/s");
    delete seis;
    return true;
}

bool FilterReadSeisHead(updateArgv* update_argv) {
    BOOST_REQUIRE_MESSAGE(update_argv!=NULL, "update_argv is null");
    SeisFile* seis = HDFS::New("/" + update_argv->seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    HeadReader* reader = seis->OpenHeadReader();
    BOOST_REQUIRE_MESSAGE(reader!=NULL, "reader is null");
    HeadFilter* filter = new HeadFilter();
    for (int i = 0; i < 64; i++) {
        filter->AddKey(i);
    }
    reader->SetHeadFilter(*filter);
    int readDat[TYPE_LENGTH / 2];
    int64_t start = 0, read_size = 0, time_cost = 0;
    PrintMessage(
            "start to read \"" + update_argv->seis_name
                    + "\" head, using \"reader->Get(k, readDat)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_argv->update_time));
    PrintMessage("read checking, start with k=" + Int642Str(update_argv->start_num));
    for (int64_t k = update_argv->start_num; k < TOTAL_ROW_NUM; k += INTERVAL) {
        start = GetCurrentMillis();
        bool read_rst = reader->Get(k, readDat);
        time_cost = time_cost + GetCurrentMillis() - start;
        BOOST_REQUIRE_MESSAGE(read_rst == true, "read_rst is false, k = " + Int642Str(k));
        int64_t t = ((k + update_argv->update_time) % THRESHOLD);
//        cout<<readDat[0]<<endl;
        BOOST_REQUIRE_MESSAGE(readDat[0] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
//        cout<<readDat[TYPE_LENGTH/2 - 1]<<endl;
        BOOST_REQUIRE_MESSAGE(readDat[TYPE_LENGTH / 2 - 1] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
//        cout<<readDat[TYPE_LENGTH / 4]<<endl;
        BOOST_REQUIRE_MESSAGE(readDat[TYPE_LENGTH / 4] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
        read_size += TOTAL_ROW_SIZE;
    }
    delete reader;
    PrintMessage("read head checking finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average read speed: " + Int642Str(read_size / time_cost / (int64_t) 1024) + "MB/s");
    delete seis;
    return true;
}

bool SkipReadSeisTrace(updateArgv* update_argv) {
    BOOST_REQUIRE_MESSAGE(update_argv!=NULL, "update_argv is null");
    SeisFile* seis = HDFS::New("/" + update_argv->seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    TraceReader* reader = seis->OpenTraceReader();
    BOOST_REQUIRE_MESSAGE(reader!=NULL, "reader is null");
//    reader->SetFilter();
    int readDat[TYPE_LENGTH];
    int64_t start = 0, read_size = 0, time_cost = 0;
    PrintMessage(
            "start to read \"" + update_argv->seis_name
                    + "\" trace, using \"reader->Get(k, readDat)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_argv->update_time));
    PrintMessage("read checking, start with k=" + Int642Str(update_argv->start_num));
    for (int64_t k = update_argv->start_num; k < TOTAL_ROW_NUM; k += INTERVAL) {
        start = GetCurrentMillis();
        bool read_rst = reader->Get(k, readDat);
        time_cost = time_cost + GetCurrentMillis() - start;
        BOOST_REQUIRE_MESSAGE(read_rst == true, "read_rst is false, k = " + Int642Str(k));
        int64_t t = ((k + update_argv->update_time) % THRESHOLD);
        BOOST_REQUIRE_MESSAGE(readDat[0] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
        BOOST_REQUIRE_MESSAGE(readDat[TYPE_LENGTH - 1] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
        BOOST_REQUIRE_MESSAGE(readDat[TYPE_LENGTH / 2] == t,
                "critical check readDat[0] == t has failed, k = " + Int642Str(k));
        read_size += TOTAL_ROW_SIZE;
    }
    if (update_argv->need_host_name) {
        std::vector<int64_t> head_id;
        std::vector<std::vector<std::string>> hostInfos;
        for (int i = 0; i < GET_HOST_NUM; i++) {
            head_id.push_back(i);
        }
        (dynamic_cast<SeisTraceReader*>(reader))->GetLocations(head_id, hostInfos);
        for (int i = 0; i < GET_HOST_NUM; i++) {
            PrintMessage("hostname for row NO." + Int642Str(i));
            int j = 0;
            for (std::vector<std::string>::iterator it = hostInfos[i].begin();
                    it != hostInfos[i].end(); it++, j++) {
                PrintMessage("    [" + Int642Str(j) + "] " + *it);
            }
        }
    }

    delete reader;
    PrintMessage("read trace checking finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average read speed: " + Int642Str(read_size / time_cost / (int64_t) 1024) + "MB/s");
    delete seis;
    return true;
}

void* SkipUpdateSeisHead(void* vUpdate_argv) {
    updateArgv* update_argv = (updateArgv*) vUpdate_argv;
    std::string seis_name = update_argv->seis_name;
    int update_time = update_argv->update_time;
    int start_num = update_argv->start_num;
    SeisFile* seis = HDFS::New("/" + seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    HeadType headType;
    for (int i = 0; i < TYPE_LENGTH; i++)
        headType.head_size.push_back(TYPE_SIZE);
    HeadUpdater* updater = seis->OpenHeadUpdater();
    PrintMessage("start to update \"" + seis_name + "\" head, using \"updater->Put(i,head)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_time));
    PrintMessage("updating, start with i=" + Int642Str(start_num));
    int64_t start = 0, update_size = 0, time_cost = 0;
    int head[headType.head_size.size()];
    for (int64_t i = start_num; i < TOTAL_ROW_NUM; i += INTERVAL) {
        for (uint j = 0; j < headType.head_size.size(); j++) {
            head[j] = ((i + update_time) % THRESHOLD);
        }
        start = GetCurrentMillis();
        updater->Put(i, head);
        time_cost = time_cost + GetCurrentMillis() - start;
        update_size++;
    }
    updater->Sync();
    time_cost = time_cost + GetCurrentMillis() - start;
    delete updater;
    delete seis;
    PrintMessage("update finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    int64_t row_size = 0;
    for (int i = 0; i < TYPE_LENGTH; i++)
        row_size += headType.head_size[i];
    PrintMessage(
            "average update speed: "
                    + Int642Str(row_size * update_size / time_cost / (int64_t) 1024) + "MB/s");
    return NULL;
}

void* SkipUpdateSeisTrace(void* vUpdate_argv) {
    updateArgv* update_argv = (updateArgv*) vUpdate_argv;
    std::string seis_name = update_argv->seis_name;
    int update_time = update_argv->update_time;
    int start_num = update_argv->start_num;
    SeisFile* seis = HDFS::New("/" + seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    TraceType traceType;
    traceType.trace_size = TYPE_LENGTH;
    TraceUpdater* updater = seis->OpenTraceUpdater();
    PrintMessage("start to update \"" + seis_name + "\" trace, using \"updater->Put(i,trace)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_time));
    PrintMessage("updating, start with i=" + Int642Str(start_num));
    int64_t start = 0, update_size = 0, time_cost = 0;
    int trace[traceType.trace_size];
    for (int64_t i = start_num; i < TOTAL_ROW_NUM; i += INTERVAL) {
        for (uint j = 0; j < traceType.trace_size; j++) {
            trace[j] = ((i + update_time) % THRESHOLD);
        }
        start = GetCurrentMillis();
        updater->Put(i, trace);
        time_cost = time_cost + GetCurrentMillis() - start;
        update_size += 4;
    }
    updater->Sync();
    time_cost = time_cost + GetCurrentMillis() - start;
    delete updater;
    delete seis;
    PrintMessage("update finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average update speed: " + Int642Str(update_size / time_cost / (int64_t) 1024)
                    + "MB/s");
    return NULL;
}

void TruncateHead(string seis_name) {
    SeisFile* seis = HDFS::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    PrintMessage("start to truncate \"" + seis_name + "\" head");
    int64_t time_cost = GetCurrentMillis();
    BOOST_REQUIRE_EQUAL(seis->TruncateHead(TOTAL_ROW_NUM / 2), true);
    time_cost = GetCurrentMillis() - time_cost;
    PrintMessage("truncate finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    TOTAL_ROW_NUM = TOTAL_ROW_NUM / 2;
    delete seis;
}

void TruncateTrace(string seis_name) {
    SeisFile* seis = HDFS::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    PrintMessage("start to truncate \"" + seis_name + "\" trace");
    int64_t time_cost = GetCurrentMillis();
    BOOST_REQUIRE_EQUAL(seis->TruncateTrace(TOTAL_ROW_NUM / 2), true);
    time_cost = GetCurrentMillis() - time_cost;
    PrintMessage("truncate finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    TOTAL_ROW_NUM = TOTAL_ROW_NUM / 2;
    delete seis;
}

void SeisCompact(string seis_name) {
    SeisFile* seis = HDFS::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    PrintMessage("start to compact \"" + seis_name + "\"");
    int64_t time_cost = GetCurrentMillis();
    BOOST_REQUIRE_EQUAL((dynamic_cast<HDFS*>(seis))->Compact(), true);
    time_cost = GetCurrentMillis() - time_cost;
    PrintMessage("compact finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    delete seis;
}

void SeisMerge(string src, string dest) {
    SeisFile* seis_dest = HDFS::New(dest);
    BOOST_REQUIRE_MESSAGE(seis_dest!=NULL, "seis is null");
    PrintMessage("start to merge \"" + src + "\" into \"" + dest + "\"");
    int64_t time_cost = GetCurrentMillis();
    BOOST_REQUIRE_EQUAL(seis_dest->Merge(src), true);
    time_cost = GetCurrentMillis() - time_cost;
    PrintMessage("merge finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    delete seis_dest;
}

void RemoveSeis(string seis_name) {
    SeisFile* seis = HDFS::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    PrintMessage("start to remove \"" + seis_name + "\"");
    int64_t time_cost = GetCurrentMillis();
    BOOST_REQUIRE_EQUAL(seis->Remove(), true);
    time_cost = GetCurrentMillis() - time_cost;
    PrintMessage("remove finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage("checking existence \"" + seis_name + "\"");
    BOOST_REQUIRE_EQUAL(!seis->Exists(), true);
    PrintMessage("\"" + seis_name + "\" does not exist, checking passed");
}

//BOOST_AUTO_TEST_CASE(seis_test_row) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName() + "_row";
//    update_argv1->update_time = 0;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = true;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//
//    TOTAL_ROW_NUM = 0;
//    WriteSeisTrace(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisTrace(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisTrace( (void*) update_argv1);
//    SkipReadSeisTrace(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}
//
//BOOST_AUTO_TEST_CASE(seis_test_cloumn) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = true;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//
//    TOTAL_ROW_NUM = 0;
//    WriteSeisTrace(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisTrace(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisTrace( (void*) update_argv1);
//    SkipReadSeisTrace(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}

//BOOST_AUTO_TEST_CASE(seis_test_trace) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = false;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisTrace(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisTrace(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisTrace( (void*) update_argv1);
//    SkipReadSeisTrace(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisTrace(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}

BOOST_AUTO_TEST_CASE(seis_test_head11) {
    updateArgv* update_argv1 = new updateArgv();
    update_argv1->seis_name = GetFileName();
    update_argv1->update_time = 1;
    update_argv1->start_num = 0;
    update_argv1->need_host_name = false;
    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);

    //for test only
//    TOTAL_WRITE_ROWS = 11;

    string path1 = "/" + update_argv1->seis_name;
    RemoveSeis(path1);
//    TOTAL_ROW_NUM += TOTAL_WRITE_ROWS;
    PrintMessage(update_argv1->seis_name + "----------begin------------------");
    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead((void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    delete update_argv1;
}

//BOOST_AUTO_TEST_CASE(seis_test_head19) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = false;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//
//    //for test only
//    TOTAL_WRITE_ROWS = 19;
//
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}
//
//BOOST_AUTO_TEST_CASE(seis_test_head21) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = false;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//
//    //for test only
//    TOTAL_WRITE_ROWS = 21;
//
//
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}
//
//BOOST_AUTO_TEST_CASE(seis_test_head29) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = false;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//
//    //for test only
//    TOTAL_WRITE_ROWS = 29;
//
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}
//
//BOOST_AUTO_TEST_CASE(seis_test_head31) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = false;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//
//    //for test only
//    TOTAL_WRITE_ROWS = 31;
//
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}
//
//BOOST_AUTO_TEST_CASE(seis_test_head39) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = false;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//
//    //for test only
//    TOTAL_WRITE_ROWS = 39;
//
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}
//
//BOOST_AUTO_TEST_CASE(seis_test_head41) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = false;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//
//    //for test only
//    TOTAL_WRITE_ROWS = 41;
//
//
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}
//
//BOOST_AUTO_TEST_CASE(seis_test_head49) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName();
//    update_argv1->update_time = 1;
//    update_argv1->start_num = 0;
//    update_argv1->need_host_name = false;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//
//
//    //for test only
//    TOTAL_WRITE_ROWS = 49;
//
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    update_argv1->update_time = 0;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->update_time = 1;
//    SkipUpdateSeisHead( (void*) update_argv1);
//    SkipReadSeisHead(update_argv1);
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}

BOOST_AUTO_TEST_SUITE_END()

