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
int64_t TYPE_SIZE;                //关键字大小        单位B
int64_t TYPE_LENGTH;              //关键字个数
int64_t TOTAL_WRITE_ROWS;         //写入的行数
int64_t TOTAL_ROW_SIZE;           //一行大小         单位B
int64_t INTERVAL;                 //读取、更新间隔行数
int64_t TOTAL_ROW_NUM;            //当前seis总行数

string message_file_name =  "./seis_test_log";     //放置打印信息的文件名
string _seis_name_gloable = "seis_test";
pthread_mutex_t file_lock;

struct updateArgv {                //更新函数的传入参数
    string seis_name;
    int update_time;              //更新的次数，与读取检测相关
    int start_num;                //开始更新或读取的行号
    bool need_host_name;
};

void SetTestParameters(int64_t write_data_size, int64_t type_size, int64_t type_length, int64_t interval) {
    WRITE_DATA_SIZE = write_data_size;
    TYPE_SIZE = type_size;
    TYPE_LENGTH = type_length;
    TOTAL_ROW_SIZE = type_size * type_length;
    TOTAL_WRITE_ROWS = (WRITE_DATA_SIZE * 1024 * 1024 * 1024 / (TYPE_SIZE * TYPE_LENGTH));
    INTERVAL = interval;
    TOTAL_ROW_NUM = 0;
}

void PrintMessage(string message) {
    cout << message << endl;
    ofstream out(message_file_name.c_str(), ios::app);
    out << message << endl;
    out.close();
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
                    + Double2Str((double)TOTAL_WRITE_ROWS * TOTAL_ROW_SIZE / time_cost / (int64_t) 1024)
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
                    + Double2Str((double)TOTAL_WRITE_ROWS * TOTAL_ROW_SIZE / time_cost / (int64_t) 1024)
                    + "MB/s");
    delete writer;
    delete seis;
    return true;
}

void* SkipReadSeisHead(void* vUpdate_argv) {
    updateArgv* update_argv = (updateArgv*) vUpdate_argv;
    BOOST_REQUIRE_MESSAGE(update_argv!=NULL, "update_argv is null");
    SeisFile* seis = HDFS::New("/" + update_argv->seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    HeadReader* reader = seis->OpenHeadReader();
    BOOST_REQUIRE_MESSAGE(reader!=NULL, "reader is null");
    int readDat[TYPE_LENGTH];
    int64_t start = 0, read_size = 0, time_cost = 0;
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
    pthread_mutex_lock(&file_lock);
    PrintMessage(
            "start to read \"" + update_argv->seis_name
                    + "\" head, using \"reader->Get(k, readDat)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_argv->update_time));
    PrintMessage("read checking, start with k=" + Int642Str(update_argv->start_num));
    PrintMessage("read head checking finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average read speed: " + Double2Str((double)read_size / time_cost / (int64_t) 1024) + "MB/s");
    pthread_mutex_unlock(&file_lock);
    delete seis;
    return NULL;
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
            "average read speed: " + Double2Str((double)read_size / time_cost / (int64_t) 1024) + "MB/s");
    delete seis;
    return true;
}

void* SkipReadSeisTrace(void* vUpdate_argv) {
    updateArgv* update_argv = (updateArgv*) vUpdate_argv;
    BOOST_REQUIRE_MESSAGE(update_argv!=NULL, "update_argv is null");
    SeisFile* seis = HDFS::New("/" + update_argv->seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    TraceReader* reader = seis->OpenTraceReader();
    BOOST_REQUIRE_MESSAGE(reader!=NULL, "reader is null");
//    reader->SetFilter();
    int readDat[TYPE_LENGTH];
    int64_t start = 0, read_size = 0, time_cost = 0;
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
    pthread_mutex_lock(&file_lock);
    PrintMessage(
            "start to read \"" + update_argv->seis_name
                    + "\" trace, using \"reader->Get(k, readDat)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_argv->update_time));
    PrintMessage("read checking, start with k=" + Int642Str(update_argv->start_num));
    PrintMessage("read trace checking finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average read speed: " + Double2Str((double)read_size / time_cost / (int64_t) 1024) + "MB/s");
    pthread_mutex_unlock(&file_lock);
    delete seis;
    return NULL;
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
    int64_t start = 0, update_size = 0, time_cost = 0;
    int head[headType.head_size.size()];
    for (int64_t i = start_num; i < TOTAL_ROW_NUM/50; i += INTERVAL) {
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
    int64_t row_size = 0;
    for (int i = 0; i < TYPE_LENGTH; i++)
        row_size += headType.head_size[i];

    pthread_mutex_lock(&file_lock);
    PrintMessage("start to update \"" + seis_name + "\" head, using \"updater->Put(i,head)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_time));
    PrintMessage("updating, start with i=" + Int642Str(start_num));
    PrintMessage("update finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average update speed: "
                    + Double2Str((double)row_size * update_size / time_cost / (int64_t) 1024) + "MB/s");
    pthread_mutex_unlock(&file_lock);
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
    pthread_mutex_lock(&file_lock);
    PrintMessage("start to update \"" + seis_name + "\" trace, using \"updater->Put(i,trace)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_time));
    PrintMessage("updating, start with i=" + Int642Str(start_num));
    PrintMessage("update finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average update speed: " + Double2Str((double)update_size / time_cost / (int64_t) 1024)
                    + "MB/s");
    pthread_mutex_unlock(&file_lock);
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

BOOST_AUTO_TEST_CASE(test_seis_write) {
    //set para
//    _seis_name_gloable+=GetNewUuid();
    message_file_name+=GetNewUuid();
    //写入总量GB 地震道类型大小B  地震道长度  验证间隔行数
    SetTestParameters(1l, 4l, 128l, 1l);

    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_write^^^^^^^^^^^^^^^^^^^^^^begin^^^^^^^^^^^^^^^^^^^^^^");

    updateArgv* update_argv1 = new updateArgv();

    update_argv1->seis_name = _seis_name_gloable;
    update_argv1->update_time = 0;
    update_argv1->start_num = 0;
    update_argv1->need_host_name = false;

    string path1 = "/" + update_argv1->seis_name;
    RemoveSeis(path1);
    WriteSeisHead(path1);
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_write^^^^^^^^^^^^^^^^^^^^^^stop^^^^^^^^^^^^^^^^^^^^^^^");
    delete update_argv1;
}

BOOST_AUTO_TEST_CASE(test_seis_read) {
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_read^^^^^^^^^^^^^^^^^^^^^^begin^^^^^^^^^^^^^^^^^^^^^^");
    updateArgv* update_argv1 = new updateArgv();
    update_argv1->seis_name = _seis_name_gloable;
    update_argv1->update_time = 0;
    update_argv1->start_num = 0;
    update_argv1->need_host_name = false;
    SkipReadSeisHead((void*)update_argv1);
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_read^^^^^^^^^^^^^^^^^^^^^^stop^^^^^^^^^^^^^^^^^^^^^^^");
    delete update_argv1;
}

BOOST_AUTO_TEST_CASE(test_seis_update) {
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_update^^^^^^^^^^^^^^^^^^^^^^begin^^^^^^^^^^^^^^^^^^^^^^");
    updateArgv* update_argv1 = new updateArgv();
    update_argv1->seis_name = _seis_name_gloable;
    update_argv1->update_time = 1;
    update_argv1->start_num = 0;
    update_argv1->need_host_name = false;
    SkipUpdateSeisHead((void*)update_argv1);
    SkipReadSeisHead((void*)update_argv1);
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_update^^^^^^^^^^^^^^^^^^^^^^stop^^^^^^^^^^^^^^^^^^^^^^^");
    delete update_argv1;
}

BOOST_AUTO_TEST_CASE(test_seis_compact) {
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_compact^^^^^^^^^^^^^^^^^^^^^^begin^^^^^^^^^^^^^^^^^^^^^^");
    updateArgv* update_argv1 = new updateArgv();
    update_argv1->seis_name = _seis_name_gloable;
    update_argv1->update_time = 1;
    update_argv1->start_num = 0;
    update_argv1->need_host_name = false;
    SeisCompact(path1);
    SkipReadSeisHead((void*)update_argv1);
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_compact^^^^^^^^^^^^^^^^^^^^^^stop^^^^^^^^^^^^^^^^^^^^^^^");
    delete update_argv1;
}

BOOST_AUTO_TEST_CASE(test_seis_muti_process_read) {
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_muti_process_read^^^^^^^^^^^^^^^^^^^^^^begin^^^^^^^^^^^^^^^^^^^^^^");
    updateArgv* update_argv1 = new updateArgv();
    update_argv1->seis_name = _seis_name_gloable;
    update_argv1->update_time = 1;
    update_argv1->start_num = 0;
    update_argv1->need_host_name = false;

    vector<pthread_t> v_thread;
    int num_node=30;
    v_thread.resize(num_node);
    for(int i=1;i<num_node;i++){
        for(int j=0;j<i+1;j++){
            pthread_create(&v_thread[j], NULL, SkipReadSeisHead, (void*) update_argv1);
        }
        for(int j=0;j<i+1;j++){
            pthread_join(v_thread[j], NULL);
        }
    }
    PrintMessage("^^^^^^^^^^^^^^^^^^^^^^test_seis_muti_process_read^^^^^^^^^^^^^^^^^^^^^^stop^^^^^^^^^^^^^^^^^^^^^^^");
    delete update_argv1;
}

BOOST_AUTO_TEST_SUITE_END()

