#include "garbage_collector_hdfs.h"

#include "gcache_tmp_file.h"
#include "gcache_tmp_writer.h"
#include "gcache_tmp_kvtable.h"
#include <iostream>

using namespace seisfs::tmp;
using namespace std;

int main() {
    GarbageCollectorHdfs* colletorHdfs = new GarbageCollectorHdfs();
    colletorHdfs->PostOrderTraversal();
    delete colletorHdfs;
    cout << "tmp hdfs garbage collection finished!" << endl;
    return 0;
}
