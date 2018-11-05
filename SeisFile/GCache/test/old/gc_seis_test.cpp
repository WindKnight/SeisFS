#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include "gc/garbage_collector_nas.h"
#include "gc/garbage_collector_local.h"
#include "gc/garbage_collector_hdfs.h"
#include "gc/garbage_collector_seis.h"

#include "tmp/gcache_tmp_file.h"
#include "tmp/gcache_tmp_writer.h"
#include "tmp/gcache_tmp_kvtable.h"
#include "seis/gcache_seis_data.h"

using namespace seisfs;
using namespace std;
using namespace boost;

BOOST_AUTO_TEST_SUITE(gc_seis_test)

void GC_Test_Create_Seis(string path, int lifetime) {
    SeisFileHDFS* seis = SeisFileHDFS::New(path);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    cout << "seis->Remove()" << seis->Remove() << endl;
    cout << "seis->Exists() is " << seis->Exists() << endl;
    HeadType* headType = new HeadType();
    headType->length = 5;
    headType->placement = BY_ROW;
    TraceType* traceType = new TraceType();
    traceType->length = 5;
    SeisTraceWriter* writer = seis->OpenTraceWriter(traceType, lifetime);
    for (int i = 0; i < 16; ++i) {
        float trace[5];
        for (int j = 0; j < 5; j++) {
            trace[j] = -i;
        }
        cout << "write result is " << writer->Write(trace) << endl;
    }
    writer->Sync();

    int64_t writer_pos = writer->Pos();
    cout << "writer pos is " << writer_pos << endl;
    writer->Close();
    delete writer;
    delete seis;
}

void GC_Test_SetMetaTag_Seis(string path) {
    SeisFileHDFS* seis = SeisFileHDFS::New(path);
    BOOST_REQUIRE_MESSAGE(seis!=NULL, "seis is null");
    BOOST_REQUIRE_EQUAL(seis->Exists(), true);
    seis->SetMetaTag(MOVE_TO_NAS);
    delete seis;
}

BOOST_AUTO_TEST_CASE(gc_seis_test_create) {
    GC_Test_Create_Seis("/A/data1", 0);
    GC_Test_Create_Seis("/A/B/data2", 2);
    GC_Test_Create_Seis("/A/C/data3", 2);
    GC_Test_Create_Seis("/A/B/D/data4", 0);
    GC_Test_Create_Seis("/A/B/E/data5", 2);
    GC_Test_Create_Seis("/A/C/F/data6", 6);
    GC_Test_Create_Seis("/A/C/G/data7", 6);
}

BOOST_AUTO_TEST_CASE(gc_seis_test_collector1) {
    GarbageCollectorSeis* colletor = new GarbageCollectorSeis();
    colletor->PostOrderTraversal(0.00001);
    delete colletor;
}

BOOST_AUTO_TEST_CASE(gc_seis_test_setMetaTag) {
    GC_Test_SetMetaTag_Seis("/A/C/data3");
    GC_Test_SetMetaTag_Seis("/A/C/F/data6");
}

BOOST_AUTO_TEST_CASE(gc_seis_test_collector2) {
    GarbageCollectorSeis* colletor = new GarbageCollectorSeis();
    colletor->PostOrderTraversal(0.00001);
    delete colletor;
}

//BOOST_AUTO_TEST_CASE(gc_hdfs_test_create) {
//	File *file1 = File::New("/A/data1", STORAGE_DIST);
//	Writer* writer1=file1->OpenWriter(2);
//
//	File *file2 = File::New("/A/B/data2", STORAGE_DIST);
//	Writer* writer2=file2->OpenWriter(2);
//
//	File *file3 = File::New("/A/C/data3", STORAGE_DIST);
//	Writer* writer3=file3->OpenWriter(2);
//
//
//	File *file4 = File::New("/A/B/D/data4", STORAGE_DIST);
//	Writer* writer4=file4->OpenWriter(2);
//
//	File *file5 = File::New("/A/B/E/data5", STORAGE_DIST);
//	Writer* writer5=file5->OpenWriter(2);
//
//	File *file6 = File::New("/A/C/F/data6", STORAGE_DIST);
//	Writer* writer6=file6->OpenWriter(6);
//
//	File *file7 = File::New("/A/C/G/data7", STORAGE_DIST);
//	Writer* writer7=file7->OpenWriter(6);
//
//	delete file1;
//	delete file2;
//	delete file3;
//	delete file4;
//	delete file5;
//	delete file6;
//	delete file7;
//
//	delete writer1;
//	delete writer2;
//	delete writer3;
//	delete writer4;
//	delete writer5;
//	delete writer6;
//	delete writer7;
//}

BOOST_AUTO_TEST_SUITE_END()