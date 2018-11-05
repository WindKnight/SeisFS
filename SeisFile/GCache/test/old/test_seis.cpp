/*
 * test_seis_column.cpp
 *
 *  Created on: 2017年5月2日
 *      Author: root
 */
#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <seis/gcache_seis_data.h>
#include <seis/gcache_seis_meta.h>
#include <seis/head/gcache_seis_headreader.h>
#include <seis/head/gcache_seis_headwriter.h>
#include <sys/types.h>
#include <unistd.h>
#include <util/gcache_scoped_pointer.h>
#include <util/gcache_string.h>
#include <ctime>
#include <iostream>
#include <fstream>
#include <thread>
#include <math.h>

using namespace seisfs;
using namespace std;
using namespace boost;

BOOST_AUTO_TEST_SUITE(seis_test)

#define THRESHOLD 2147483647

int64_t WRITE_DATA_SIZE;          //写入的总数据量    单位GB
int64_t FILE_PART_SIZE;           //文件分块大小      单位GB
int64_t TYPE_SIZE;                //关键字大小        单位B
int64_t TYPE_LENGTH;              //关键字个数
int64_t TOTAL_WRITE_ROWS;         //写入的行数
int64_t FILE_MAX_ROWS;            //一个文件的最大行数
int64_t TOTAL_ROW_SIZE;           //一行大小         单位B
int64_t INTERVAL;                 //读取、更新间隔行数
int64_t TOTAL_ROW_NUM;            //当前seis总行数
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
    TOTAL_WRITE_ROWS = 10;
    FILE_MAX_ROWS = (FILE_PART_SIZE * 1024 * 1024 * 1024 / (TYPE_SIZE * TYPE_LENGTH));
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
    SeisData* seis = SeisData::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    ScopedPointer < HeadType > headType(new HeadType());
    int *headSize = new int[TYPE_LENGTH];
    for (int i = 0; i < TYPE_LENGTH; i++)
        headSize[i] = TYPE_SIZE;
    headType->length = TYPE_LENGTH;
    headType->placement = PLACEMENT;
    headType->head_size = headSize;
    SeisHeadWriter* writer = seis->OpenHeadWriter(headType.data(), 20, FILE_MAX_ROWS);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    PrintMessage("start to write \"" + seis_name + "\" head");
    int head[headType->length];
    int64_t start = 0, time_cost = 0;
    int64_t writer_pos = writer->Pos();
    for (int64_t i = writer_pos; i < writer_pos + TOTAL_WRITE_ROWS; ++i) {
        for (uint j = 0; j < headType->length; j++) {
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
    SeisData* seis = SeisData::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    ScopedPointer < TraceType > traceType(new TraceType());
    traceType->length = TYPE_LENGTH;
    SeisTraceWriter* writer = seis->OpenTraceWriter(traceType.data(), 20, FILE_MAX_ROWS);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    PrintMessage("start to write \"" + seis_name + "\" trace");
    int head[traceType->length];
    int64_t start = 0, time_cost = 0;
    int64_t writer_pos = writer->Pos();
    for (int64_t i = writer_pos; i < writer_pos + TOTAL_WRITE_ROWS; ++i) {
        for (uint j = 0; j < traceType->length; j++) {
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
    SeisData* seis = SeisData::New("/" + update_argv->seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    ScopedPointer < SeisHeadReader > reader(seis->OpenHeadReader());
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
        for (int i = 0; i < 20; i++) {
            head_id.push_back(i);
        }
        reader->GetLocations(head_id, hostInfos);
        for (int i = 0; i < 20; i++) {
            PrintMessage("hostname for row NO." + Int642Str(i));
            int j = 0;
            for (std::vector<std::string>::iterator it = hostInfos[i].begin();
                    it != hostInfos[i].end(); it++, j++) {
                PrintMessage("    [" + Int642Str(j) + "] " + *it);
            }
        }
    }

    PrintMessage("read head checking finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average read speed: " + Int642Str(read_size / time_cost / (int64_t) 1024) + "MB/s");
    delete seis;
    return true;
}

bool FilterReadSeisHead(updateArgv* update_argv) {
    BOOST_REQUIRE_MESSAGE(update_argv!=NULL, "update_argv is null");
    SeisData* seis = SeisData::New("/" + update_argv->seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    ScopedPointer < SeisHeadReader > reader(seis->OpenHeadReader());
    BOOST_REQUIRE_MESSAGE(reader!=NULL, "reader is null");
    SeisHeadFilter* filter = new SeisHeadFilter();
    for (int i = 0; i < 64; i++) {
        filter->AddKey(i);
    }
    reader->SetFilter(filter);
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
    PrintMessage("read head checking finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    PrintMessage(
            "average read speed: " + Int642Str(read_size / time_cost / (int64_t) 1024) + "MB/s");
    delete seis;
    return true;
}

bool SkipReadSeisTrace(updateArgv* update_argv) {
    BOOST_REQUIRE_MESSAGE(update_argv!=NULL, "update_argv is null");
    SeisData* seis = SeisData::New("/" + update_argv->seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    ScopedPointer < SeisTraceReader > reader(seis->OpenTraceReader());
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
        for (int i = 0; i < 20; i++) {
            head_id.push_back(i);
        }
        reader->GetLocations(head_id, hostInfos);
        for (int i = 0; i < 20; i++) {
            PrintMessage("hostname for row NO." + Int642Str(i));
            int j = 0;
            for (std::vector<std::string>::iterator it = hostInfos[i].begin();
                    it != hostInfos[i].end(); it++, j++) {
                PrintMessage("    [" + Int642Str(j) + "] " + *it);
            }
        }
    }

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
    SeisData* seis = SeisData::New("/" + seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    ScopedPointer < HeadType > headType(new HeadType());
    int *headSize = new int[TYPE_LENGTH];
    for (int i = 0; i < TYPE_LENGTH; i++)
        headSize[i] = TYPE_SIZE;
    headType->length = TYPE_LENGTH;
    headType->placement = PLACEMENT;
    headType->head_size = headSize;
    SeisHeadUpdater* updater = seis->OpenHeadUpdater();
    PrintMessage("start to update \"" + seis_name + "\" head, using \"updater->Put(i,head)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_time));
    PrintMessage("updating, start with i=" + Int642Str(start_num));
    int64_t start = 0, update_size = 0, time_cost = 0;
    int head[headType->length];
    for (int64_t i = start_num; i < TOTAL_ROW_NUM; i += INTERVAL) {
        for (uint j = 0; j < headType->length; j++) {
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
        row_size += headSize[i];
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
    SeisData* seis = SeisData::New("/" + seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    ScopedPointer < TraceType > traceType(new TraceType());
    traceType->length = TYPE_LENGTH;
    SeisTraceUpdater* updater = seis->OpenTraceUpdater();
    PrintMessage("start to update \"" + seis_name + "\" trace, using \"updater->Put(i,trace)\"");
    PrintMessage("INTERVAL is: " + Int642Str(INTERVAL));
    PrintMessage("update_time is: " + Int642Str(update_time));
    PrintMessage("updating, start with i=" + Int642Str(start_num));
    int64_t start = 0, update_size = 0, time_cost = 0;
    int trace[traceType->length];
    for (int64_t i = start_num; i < TOTAL_ROW_NUM; i += INTERVAL) {
        for (uint j = 0; j < traceType->length; j++) {
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
    SeisData* seis = SeisData::New(seis_name);
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
    SeisData* seis = SeisData::New(seis_name);
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
    SeisData* seis = SeisData::New(seis_name);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    PrintMessage("start to compact \"" + seis_name + "\"");
    int64_t time_cost = GetCurrentMillis();
    BOOST_REQUIRE_EQUAL(seis->Compact(), true);
    time_cost = GetCurrentMillis() - time_cost;
    PrintMessage("compact finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    delete seis;
}

void SeisMerge(string src, string dest) {
    SeisData* seis_dest = SeisData::New(dest);
    BOOST_REQUIRE_MESSAGE(seis_dest!=NULL, "seis is null");
    PrintMessage("start to merge \"" + src + "\" into \"" + dest + "\"");
    int64_t time_cost = GetCurrentMillis();
    ReelMerge *merger = NULL;
    BOOST_REQUIRE_EQUAL(seis_dest->Merge(src, merger), true);
    time_cost = GetCurrentMillis() - time_cost;
    PrintMessage("merge finished");
    PrintMessage("time cost: " + Int642Str(time_cost) + "ms");
    delete merger;
    delete seis_dest;
}

void RemoveSeis(string seis_name) {
    SeisData* seis = SeisData::New(seis_name);
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

BOOST_AUTO_TEST_CASE(seis_test_gettrace_num) {
    updateArgv* update_argv1 = new updateArgv();
    update_argv1->seis_name = GetFileName() + "_column";
    update_argv1->update_time = 0;
    update_argv1->start_num = 0;
    update_argv1->need_host_name = true;
//    int64_t tmp_interval;
    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//    tmp_interval = INTERVAL;
    string path1 = "/" + update_argv1->seis_name;
    RemoveSeis(path1);
    PrintMessage(update_argv1->seis_name + "----------begin------------------");
    WriteSeisHead(path1);
    TOTAL_ROW_NUM = 0;
    WriteSeisTrace(path1);

    string path2 = "/" + update_argv1->seis_name + "_merge_dest";
    TOTAL_WRITE_ROWS = 7;
    RemoveSeis(path2);
    TOTAL_ROW_NUM = 0;
    WriteSeisHead(path2);
    TOTAL_ROW_NUM = 0;
    TOTAL_WRITE_ROWS = 30;
    WriteSeisTrace(path2);
    SeisMerge(path1, path2);

    string path3 = "/" + update_argv1->seis_name + "wdsawd";
    TOTAL_WRITE_ROWS = 553;
    RemoveSeis(path3);
    TOTAL_ROW_NUM = 0;
    WriteSeisHead(path3);
    TOTAL_ROW_NUM = 0;
    TOTAL_WRITE_ROWS = 330;
    WriteSeisTrace(path3);
    SeisMerge(path2, path3);

    SeisData* seis = SeisData::New(path3);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    cout << "traceNum should be " << 330 + 30 + 10 << endl;
    cout << "traceNum is " << seis->GetTraceNum() << endl;
    delete seis;

    update_argv1->seis_name = update_argv1->seis_name + "wdsawd";
    SkipReadSeisHead(update_argv1);

    SkipReadSeisTrace(update_argv1);

//    TOTAL_ROW_NUM += TOTAL_WRITE_ROWS;
//    update_argv1->update_time = 1;
//    SkipReadSeisHead(update_argv1);
//    FilterReadSeisHead(update_argv1);
//    TruncateHead(path1);
//    SkipReadSeisHead(update_argv1);
//    WriteSeisHead(path1);
//    INTERVAL = 1;
//    SkipReadSeisHead(update_argv1);
//    INTERVAL = tmp_interval;
//    pthread_t thread1;
//    pthread_t thread2;
//    INTERVAL = 2;
//    update_argv1->update_time = 1;
//    pthread_create(&thread1, NULL, SkipUpdateSeisHead, (void*) update_argv1);
//    updateArgv* update_argv2 = new updateArgv();
//    update_argv2->seis_name = update_argv1->seis_name;
//    update_argv2->update_time = 1;
//    update_argv2->start_num = 1;
//    pthread_create(&thread2, NULL, SkipUpdateSeisHead, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    INTERVAL = 1;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->start_num = 0;
//    update_argv1->update_time = 0;
//    pthread_create(&thread1, NULL, SkipUpdateSeisHead, (void*) update_argv1);
//    pthread_join(thread1, NULL);
//    INTERVAL = tmp_interval;
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);

//    TOTAL_ROW_NUM = 0;
//    update_argv1->seis_name = GetFileName() + "_merge_dest";
//    update_argv2->seis_name = GetFileName() + "_merge_dest";
//    string path2 = "/" + update_argv1->seis_name;
//    RemoveSeis(path2);
//    WriteSeisHead(path2);
//    update_argv1->update_time = 2;
//    update_argv1->start_num = 5;
//    pthread_create(&thread1, NULL, SkipUpdateSeisHead, (void*) update_argv1);
//    update_argv2->update_time = 1;
//    update_argv2->start_num = 7;
//    pthread_create(&thread2, NULL, SkipUpdateSeisHead, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    SkipReadSeisHead(update_argv1);
//    SkipReadSeisHead(update_argv2);
//    update_argv1->update_time = 0;
//    update_argv2->update_time = 0;
//    pthread_create(&thread1, NULL, SkipUpdateSeisHead, (void*) update_argv1);
//    pthread_create(&thread2, NULL, SkipUpdateSeisHead, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    WriteSeisHead(path2);
//    SeisMerge(path1, path2);
//    SkipReadSeisHead(update_argv1);
//    RemoveSeis(path2);
    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv2;
    delete update_argv1;
}

//BOOST_AUTO_TEST_CASE(seis_test_gettrace_num) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName() + "_column";
//    update_argv1->update_time = 0;
//    update_argv1->start_num = 0;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    TOTAL_ROW_NUM = 0;
//    WriteSeisTrace(path1);
//
//    string path2 = "/" + update_argv1->seis_name + "_merge_dest";
//    TOTAL_WRITE_ROWS = 7;
//    RemoveSeis(path2);
//    TOTAL_ROW_NUM = 0;
//    WriteSeisHead(path2);
//    TOTAL_ROW_NUM = 0;
//    TOTAL_WRITE_ROWS = 30;
//    WriteSeisTrace(path2);
//    SeisMerge(path1, path2);
//
//
//    string path3 = "/" + update_argv1->seis_name + "wdsawd";
//    TOTAL_WRITE_ROWS = 553;
//    RemoveSeis(path3);
//    TOTAL_ROW_NUM = 0;
//    WriteSeisHead(path3);
//    TOTAL_ROW_NUM = 0;
//    TOTAL_WRITE_ROWS = 330;
//    WriteSeisTrace(path3);
//    SeisMerge(path2, path3);
//
//    SeisData* seis = SeisData::New(path3);
//    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
//    cout<<"traceNum should be "<<330+30+10<<endl;
//    cout<<"traceNum is "<<seis->GetTraceNum()<<endl;
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv1;
//}

//BOOST_AUTO_TEST_CASE(seis_test_row) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName() + "_row";
//    update_argv1->update_time = 0;
//    update_argv1->start_num = 0;
//    int64_t tmp_interval;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 128l, 1l, BY_ROW);
//    tmp_interval = INTERVAL;
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisHead(path1);
//    SkipReadSeisHead(update_argv1);
//    TruncateHead(path1);
//    SkipReadSeisHead(update_argv1);
//    WriteSeisHead(path1);
//    INTERVAL = 1;
//    SkipReadSeisHead(update_argv1);
//    INTERVAL = tmp_interval;
//    pthread_t thread1;
//    pthread_t thread2;
//    INTERVAL = 2;
//    update_argv1->update_time = 1;
//    pthread_create(&thread1, NULL, SkipUpdateSeisHead, (void*) update_argv1);
//    updateArgv* update_argv2 = new updateArgv();
//    update_argv2->seis_name = update_argv1->seis_name;
//    update_argv2->update_time = 1;
//    update_argv2->start_num = 1;
//    pthread_create(&thread2, NULL, SkipUpdateSeisHead, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    INTERVAL = 1;
//    SkipReadSeisHead(update_argv1);
//    update_argv1->start_num = 0;
//    update_argv1->update_time = 0;
//    pthread_create(&thread1, NULL, SkipUpdateSeisHead, (void*) update_argv1);
//    pthread_join(thread1, NULL);
//    INTERVAL = tmp_interval;
//    SeisCompact(path1);
//    SkipReadSeisHead(update_argv1);
//
//    TOTAL_ROW_NUM = 0;
//    update_argv1->seis_name = GetFileName() + "_merge_dest";
//    update_argv2->seis_name = GetFileName() + "_merge_dest";
//    string path2 = "/" + update_argv1->seis_name;
//    RemoveSeis(path2);
//    WriteSeisHead(path2);
//    update_argv1->update_time = 2;
//    update_argv1->start_num = 5;
//    pthread_create(&thread1, NULL, SkipUpdateSeisHead, (void*) update_argv1);
//    update_argv2->update_time = 1;
//    update_argv2->start_num = 7;
//    pthread_create(&thread2, NULL, SkipUpdateSeisHead, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    SkipReadSeisHead(update_argv1);
//    SkipReadSeisHead(update_argv2);
//    update_argv1->update_time = 0;
//    update_argv2->update_time = 0;
//    pthread_create(&thread1, NULL, SkipUpdateSeisHead, (void*) update_argv1);
//    pthread_create(&thread2, NULL, SkipUpdateSeisHead, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    WriteSeisHead(path2);
//    SeisMerge(path1, path2);
//    SkipReadSeisHead(update_argv1);
//    RemoveSeis(path2);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv2;
//    delete update_argv1;
//}

//BOOST_AUTO_TEST_CASE(seis_test_trace) {
//    updateArgv* update_argv1 = new updateArgv();
//    update_argv1->seis_name = GetFileName() + "_trace";
//    update_argv1->update_time = 0;
//    update_argv1->start_num = 0;
//    int64_t tmp_interval;
//    //写入总量GB  分块大小GB  地震道类型大小B  地震道长度  验证间隔行数 道头排列顺序
//    SetTestParameters(1l, 1l, 4l, 2000l, 20l);
//    tmp_interval = INTERVAL;
//    string path1 = "/" + update_argv1->seis_name;
//    RemoveSeis(path1);
//    PrintMessage(update_argv1->seis_name + "----------begin------------------");
//    WriteSeisTrace(path1);
//    SkipReadSeisTrace(update_argv1);
//    TruncateTrace(path1);
//    SkipReadSeisTrace(update_argv1);
//    WriteSeisTrace(path1);
//    INTERVAL = 1;
//    SkipReadSeisTrace(update_argv1);
//    INTERVAL = tmp_interval;
//    pthread_t thread1;
//    pthread_t thread2;
//    INTERVAL = 2;
//    update_argv1->update_time = 1;
//    pthread_create(&thread1, NULL, SkipUpdateSeisTrace, (void*) update_argv1);
//    updateArgv* update_argv2 = new updateArgv();
//    update_argv2->seis_name = update_argv1->seis_name;
//    update_argv2->update_time = 1;
//    update_argv2->start_num = 1;
//    pthread_create(&thread2, NULL, SkipUpdateSeisTrace, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    INTERVAL = 1;
//    SkipReadSeisTrace(update_argv1);
//    update_argv1->start_num = 0;
//    update_argv1->update_time = 0;
//    pthread_create(&thread1, NULL, SkipUpdateSeisTrace, (void*) update_argv1);
//    pthread_join(thread1, NULL);
//    INTERVAL = tmp_interval;
//    SeisCompact(path1);
//    SkipReadSeisTrace(update_argv1);
//
//    TOTAL_ROW_NUM = 0;
//    update_argv1->seis_name = GetFileName() + "_merge_dest";
//    update_argv2->seis_name = GetFileName() + "_merge_dest";
//    string path2 = "/" + update_argv1->seis_name;
//    RemoveSeis(path2);
//    WriteSeisTrace(path2);
//    update_argv1->update_time = 2;
//    update_argv1->start_num = 5;
//    pthread_create(&thread1, NULL, SkipUpdateSeisTrace, (void*) update_argv1);
//    update_argv2->update_time = 1;
//    update_argv2->start_num = 7;
//    pthread_create(&thread2, NULL, SkipUpdateSeisTrace, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    SkipReadSeisTrace(update_argv1);
//    SkipReadSeisTrace(update_argv2);
//    update_argv1->update_time = 0;
//    update_argv2->update_time = 0;
//    pthread_create(&thread1, NULL, SkipUpdateSeisTrace, (void*) update_argv1);
//    pthread_create(&thread2, NULL, SkipUpdateSeisTrace, (void*) update_argv2);
//    pthread_join(thread1, NULL);
//    pthread_join(thread2, NULL);
//    WriteSeisTrace(path2);
//    SeisMerge(path1, path2);
//    SkipReadSeisTrace(update_argv1);
////    RemoveSeis(path2);
//    PrintMessage("stop^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//    delete update_argv2;
//    delete update_argv1;
//}

BOOST_AUTO_TEST_SUITE_END()

