#include "garbage_collector_nas.h"

#include "gcache_tmp_file.h"
#include "gcache_tmp_writer.h"
#include "gcache_tmp_kvtable.h"
#include <iostream>

using namespace seisfs::tmp;
using namespace std;

int main() {
    GarbageCollectorNas* colletorNas = new GarbageCollectorNas();
    colletorNas->PostOrderTraversal();
    delete colletorNas;
    cout << "tmp nas garbage collection finished!" << endl;
    return 0;
}
