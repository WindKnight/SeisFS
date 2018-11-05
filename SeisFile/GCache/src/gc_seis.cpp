#include "garbage_collector_seis.h"
#include "gcache_seis_data.h"
#include <iostream>

using namespace seisfs::file;
using namespace std;

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cout << "Usage: gc_tmp_hdfs [seis_cap_ratio]" << endl;
        return -1;
    }
    string val = argv[1];
    double result;
    char trash;
    sscanf(val.c_str(), "%lf%c", &result, &trash);

    GarbageCollectorSeis* colletor = new GarbageCollectorSeis();
    colletor->PostOrderTraversal(result);
    delete colletor;
    cout << "seis garbage collection finished!" << endl;
    return 0;
}
