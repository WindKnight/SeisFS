#include "garbage_collector_local.h"

#include "gcache_tmp_file.h"
#include "gcache_tmp_writer.h"
#include "gcache_tmp_kvtable.h"
#include <iostream>

using namespace seisfs::tmp;
using namespace std;

int main() {
    GarbageCollectorLocal* colletorLocal = new GarbageCollectorLocal();
    colletorLocal->PostOrderTraversal();
    delete colletorLocal;
    cout << "tmp local garbage collection finished!" << endl;
    return 0;
}
