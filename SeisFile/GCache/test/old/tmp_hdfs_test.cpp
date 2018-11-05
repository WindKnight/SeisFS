/*
 * tmp_test.cpp
 *
 *  Created on: Oct 11, 2017
 *      Author: nick
 */
#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include "tmp/gcache_advice.h"
#include "tmp/gcache_tmp_dir.h"
#include "tmp/gcache_tmp_file.h"
#include "tmp/gcache_tmp_reader.h"
#include "tmp/gcache_tmp_writer.h"
#include "tmp/gcache_tmp_kvtable.h"

using namespace std;
using namespace seisfs;
using namespace boost;

StorageArch storageArch = STORAGE_DIST;

BOOST_AUTO_TEST_SUITE(tmp_hdfs_test)

BOOST_AUTO_TEST_CASE(tmp_hdfs_1) {
    File *file = File::New("/ice/data4", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");
    Writer* writer = file->OpenWriter(10);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    string data = "0123456789abcdef";
    BOOST_REQUIRE_EQUAL(writer->Write(data.c_str(), 17), 17);
    BOOST_REQUIRE_EQUAL(writer->Seek(2, SEEK_SET), false);
    BOOST_REQUIRE_EQUAL(writer->Truncate(4), false);
    BOOST_REQUIRE_EQUAL(writer->Pos(), 17);

    writer->Sync();

    BOOST_REQUIRE_EQUAL(file->Exists(), true);
    BOOST_REQUIRE_EQUAL(file->Size(), 0);
    BOOST_REQUIRE_EQUAL(file->Truncate(10), false);
    BOOST_REQUIRE_EQUAL(file->Size(), 0);
    cout << "file GetName is " << file->GetName() << endl;
    cout << "file LastModified is " << file->LastModified() << endl;
    BOOST_REQUIRE_EQUAL(file->Lifetime(), 10);
    BOOST_REQUIRE_EQUAL(file->SetLifetime(14), true);
    BOOST_REQUIRE_EQUAL(file->Lifetime(), 14);
    BOOST_REQUIRE_EQUAL(file->LifetimeExpired(), false);
    BOOST_REQUIRE_EQUAL(file->Link("/ice/linkdata4"), false);
    BOOST_REQUIRE_EQUAL(file->Copy("/ice/sameleveldata4"), false);
    BOOST_REQUIRE_EQUAL(file->Copy("/ice/anotherleveldata4", CACHE_L2), false);

    //destroy writer,file will be closed.
    delete writer;

    cout << "-----delete writer" << endl;

    BOOST_REQUIRE_EQUAL(file->Exists(), true);
    BOOST_REQUIRE_EQUAL(file->Size(), 17);
    BOOST_REQUIRE_EQUAL(file->Truncate(10), true);
    sleep(10);   //if the program doesn't sleep reader can still read the data has been truncated.
    BOOST_REQUIRE_EQUAL(file->Size(), 10);
    cout << "file GetName is " << file->GetName() << endl;
    cout << "file LastModified is " << file->LastModified() << endl;
    BOOST_REQUIRE_EQUAL(file->Lifetime(), 14);
    BOOST_REQUIRE_EQUAL(file->SetLifetime(20), true);
    BOOST_REQUIRE_EQUAL(file->Lifetime(), 20);
    BOOST_REQUIRE_EQUAL(file->LifetimeExpired(), false);
    BOOST_REQUIRE_EQUAL(file->Link("/ice/linkdata4"), false);
    BOOST_REQUIRE_EQUAL(file->Copy("/ice/sameleveldata4"), true);
    BOOST_REQUIRE_EQUAL(file->Copy("/ice/anotherleveldata4", CACHE_L2), true);
    BOOST_REQUIRE_EQUAL(file->Rename("/ice/newdata4"), true);

    Reader* reader = file->OpenReader();
    BOOST_REQUIRE_MESSAGE(reader!=NULL, "reader is null");
    char datArray[11];
    char datLine[11];
    string strLine;

    BOOST_REQUIRE_EQUAL(reader->Read(datArray, 11), 10);
    BOOST_REQUIRE_EQUAL(datArray, "0123456789");

    BOOST_REQUIRE_EQUAL(reader->Seek(0, SEEK_SET), true);
    BOOST_REQUIRE_EQUAL(reader->ReadLine(datLine, 11), 10);
    BOOST_REQUIRE_EQUAL(datArray, "0123456789");

    BOOST_REQUIRE_EQUAL(reader->Seek(5, SEEK_SET), true);
    BOOST_REQUIRE_EQUAL(reader->ReadLine(strLine), 5);
    BOOST_REQUIRE_EQUAL(strLine.c_str(), "56789");

    BOOST_REQUIRE_EQUAL(file->Remove(), true);

    delete reader;
    delete file;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_2) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }
    char *data;
    int64_t data_size;
    for (int i = 0; i < 1000; ++i) {
        char key[10];
        sprintf(key, "key %d", i);
        BOOST_REQUIRE_EQUAL(table->Get(key, data, data_size), true);
        printf("key=%s,data=%s,dataSize=%ld\n", key, data, data_size);
    }

    std::list<std::string> key_list = table->GetKeys();
    printf("key_list size %d\n", key_list.size());
    for (std::list<std::string>::iterator list_iter = key_list.begin(); list_iter != key_list.end();
            list_iter++) {
        printf("key=%s\n", (*list_iter).c_str());
    }
    delete table;
    delete data;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_3) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }
    int64_t data_size;
    for (int i = 0; i < 990; ++i) {
        char key[10];
        sprintf(key, "key %d", i);
        BOOST_REQUIRE_EQUAL(table->Delete(key), true);
    }

    std::list<std::string> key_list = table->GetKeys();
    printf("key_list size %d\n", key_list.size());
    for (std::list<std::string>::iterator list_iter = key_list.begin(); list_iter != key_list.end();
            list_iter++) {
        char *data;
        int64_t dataSize;
        BOOST_REQUIRE_EQUAL(table->Get(*list_iter, data, dataSize), true);
        printf("key=%s,data=%s,dataSize=%d\n", list_iter->c_str(), data, dataSize);
        delete data;
    }

    delete table;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_4) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);
    BOOST_REQUIRE_EQUAL(table->Exists(), true);

    std::string tableName = table->GetName();
    printf("tableName is %s\n", tableName.c_str());
    BOOST_REQUIRE_EQUAL(table->Rename("NewTravelTime"), true);
    tableName = table->GetName();
    printf("table new name is %s\n", tableName.c_str());
    BOOST_REQUIRE_EQUAL(table->Remove(), true);

    BOOST_REQUIRE_EQUAL(table->Exists(), false);
    delete table;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_5) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }

    BOOST_REQUIRE_EQUAL(table->Lifetime(), 30);
    BOOST_REQUIRE_EQUAL(table->SetLifetime(10), true);
    BOOST_REQUIRE_EQUAL(table->Lifetime(), 10);

    BOOST_REQUIRE_EQUAL(table->LifetimeExpired(), false);

    BOOST_REQUIRE_EQUAL(table->Size(), 16890);
    BOOST_REQUIRE_EQUAL(table->SizeOf("key 100"), 17);
    BOOST_REQUIRE_EQUAL(table->Remove(), true);
    delete table;
}

BOOST_AUTO_TEST_CASE(tmp_hdfs_6) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);
    BOOST_REQUIRE_EQUAL(table->Exists(), true);

    std::string tableName = table->GetName();
    printf("tableName is %s\n", tableName.c_str());
    table->Rename("NewTravelTime");
    tableName = table->GetName();
    printf("table new name is %s\n", tableName.c_str());

    BOOST_REQUIRE_EQUAL(table->Remove(), true);

    BOOST_REQUIRE_EQUAL(table->Exists(), false);
    BOOST_REQUIRE_EQUAL(table->Remove(), false);
    delete table;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_7) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }
    char *data;
    int64_t data_size;
    for (int i = 0; i < 1000; ++i) {
        char key[10];
        sprintf(key, "key %d", i);
        BOOST_REQUIRE_EQUAL(table->Get(key, data, data_size), true);
        printf("key=%s,data=%s,dataSize=%ld\n", key, data, data_size);
    }

    std::list<std::string> key_list = table->GetKeys();
    printf("key_list size %d\n", key_list.size());
    for (std::list<std::string>::iterator list_iter = key_list.begin(); list_iter != key_list.end();
            list_iter++) {
        printf("key=%s\n", (*list_iter).c_str());
    }
    delete table;
    delete data;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_8) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }
    int64_t data_size;
    for (int i = 0; i < 990; ++i) {
        char key[10];
        sprintf(key, "key %d", i);
        BOOST_REQUIRE_EQUAL(table->Delete(key), true);
    }

    std::list<std::string> key_list = table->GetKeys();
    printf("key_list size %d\n", key_list.size());
    for (std::list<std::string>::iterator list_iter = key_list.begin(); list_iter != key_list.end();
            list_iter++) {
        char *data;
        int64_t dataSize;
        BOOST_REQUIRE_EQUAL(table->Get(*list_iter, data, dataSize), true);
        printf("key=%s,data=%s,dataSize=%d\n", list_iter->c_str(), data, dataSize);
        delete data;
    }

    delete table;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_9) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);
    BOOST_REQUIRE_EQUAL(table->Exists(), true);

    std::string tableName = table->GetName();
    printf("tableName is %s\n", tableName.c_str());
    BOOST_REQUIRE_EQUAL(table->Rename("NewTravelTime"), true);
    tableName = table->GetName();
    printf("table new name is %s\n", tableName.c_str());
    BOOST_REQUIRE_EQUAL(table->Remove(), true);

    BOOST_REQUIRE_EQUAL(table->Exists(), false);
    delete table;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_10) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }

    BOOST_REQUIRE_EQUAL(table->Lifetime(), 30);
    BOOST_REQUIRE_EQUAL(table->SetLifetime(10), true);
    BOOST_REQUIRE_EQUAL(table->Lifetime(), 10);

    BOOST_REQUIRE_EQUAL(table->LifetimeExpired(), false);

    BOOST_REQUIRE_EQUAL(table->Size(), 16890);
    BOOST_REQUIRE_EQUAL(table->SizeOf("key 100"), 17);
    BOOST_REQUIRE_EQUAL(table->Remove(), true);
    delete table;
}

BOOST_AUTO_TEST_CASE(tmp_hdfs_11) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);
    BOOST_REQUIRE_EQUAL(table->Exists(), true);

    std::string tableName = table->GetName();
    printf("tableName is %s\n", tableName.c_str());
    table->Rename("NewTravelTime");
    tableName = table->GetName();
    printf("table new name is %s\n", tableName.c_str());

    BOOST_REQUIRE_EQUAL(table->Remove(), true);

    BOOST_REQUIRE_EQUAL(table->Exists(), false);
    BOOST_REQUIRE_EQUAL(table->Remove(), false);
    delete table;

}

BOOST_AUTO_TEST_CASE(tmp_hdfs_12) {
    Dir* dir = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dir!=NULL, "dir is null");
    Dir* newDir = Dir::New("/newfolder", storageArch);
    BOOST_REQUIRE_MESSAGE(newDir!=NULL, "newDir is null");

    BOOST_REQUIRE_EQUAL(dir->Exist(), false);
    BOOST_REQUIRE_EQUAL(dir->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(newDir->Exist(), false);
    BOOST_REQUIRE_EQUAL(newDir->MakeDirs(), true);

    File* file = File::New("/folder/testfile", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");
    Writer* writer = file->OpenWriter();
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    BOOST_REQUIRE_EQUAL(writer->Write("0123456789", 10), 10);

    delete writer;
    delete file;

    BOOST_REQUIRE_EQUAL(dir->Exist(), true);
    cout << "dir GetFreeSpace is " << dir->GetFreeSpace() << endl;
    cout << "dir GetTotalSpace is " << dir->GetTotalSpace() << endl;
    BOOST_REQUIRE_EQUAL(dir->Rename("/folder1"), true);
    BOOST_REQUIRE_EQUAL(dir->Move("/newfolder"), true);

    delete dir;

    BOOST_REQUIRE_MESSAGE(newDir->Remove(true), true);
    delete newDir;
}

BOOST_AUTO_TEST_CASE(tmp_hdfs_13) {
    Dir* dir1 = Dir::New("/dir1", storageArch);
    BOOST_REQUIRE_MESSAGE(dir1!=NULL, "dir1 is null");
    Dir* dir2 = Dir::New("/dir1/dir2", storageArch);
    BOOST_REQUIRE_MESSAGE(dir2!=NULL, "dir2 is null");
    Dir* dir3 = Dir::New("/dir1/dir2/dir3", storageArch);
    BOOST_REQUIRE_MESSAGE(dir3!=NULL, "dir3 is null");
    Dir* dir4 = Dir::New("/dir4", storageArch);
    BOOST_REQUIRE_MESSAGE(dir4!=NULL, "dir4 is null");

    BOOST_REQUIRE_EQUAL(dir1->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(dir2->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(dir3->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(dir4->MakeDirs(), true);

    File* file1 = File::New("/dir1/file1", storageArch);
    BOOST_REQUIRE_MESSAGE(file1!=NULL, "file1 is null");
    Writer* writer1 = file1->OpenWriter(10);
    BOOST_REQUIRE_MESSAGE(writer1!=NULL, "writer1 is null");
    BOOST_REQUIRE_EQUAL(writer1->Write("0123456789", 10), 10);
    delete writer1;
    delete file1;

    File* file2 = File::New("/dir1/dir2/file2", storageArch);
    BOOST_REQUIRE_MESSAGE(file2!=NULL, "file2 is null");
    Writer* writer2 = file2->OpenWriter(10);
    BOOST_REQUIRE_MESSAGE(writer2!=NULL, "writer2 is null");
    BOOST_REQUIRE_EQUAL(writer2->Write("0123456789", 10), 10);
    delete writer2;
    delete file2;

    File* file3 = File::New("/dir1/dir2/dir3/file3", storageArch);
    BOOST_REQUIRE_MESSAGE(file3!=NULL, "file3 is null");
    Writer* writer3 = file3->OpenWriter(10);
    BOOST_REQUIRE_MESSAGE(writer3!=NULL, "writer3 is null");
    BOOST_REQUIRE_EQUAL(writer3->Write("0123456789", 10), 10);
    delete writer3;
    delete file3;

    BOOST_REQUIRE_EQUAL(dir1->Copy("/dir4"), true);
    BOOST_REQUIRE_EQUAL(dir1->Remove(), false);
    BOOST_REQUIRE_EQUAL(dir1->Remove(true), true);
    BOOST_REQUIRE_EQUAL(dir4->Remove(true), true);

    delete dir1;
    delete dir2;
    delete dir3;
    delete dir4;

}

BOOST_AUTO_TEST_SUITE_END()