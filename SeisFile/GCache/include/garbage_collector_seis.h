/*
 * garbage_collector_seis.h
 *
 *  Created on: Dec 19, 2016
 *      Author: node1
 */

#ifndef GARBAGE_COLLECTOR_SEIS_H_
#define GARBAGE_COLLECTOR_SEIS_H_

#include <map>
#include <string>
#include <vector>
#include <list>
#include "tree_node.h"

#define META_NAME "seis.meta"

using namespace std;

NAMESPACE_BEGIN_SEISFS
namespace file {

/**
 * this class provides some function of expired hdfs files remove.
 */
class GarbageCollectorSeis {
private:
    string _meta_root_dir; //the pure path(contains no ip address and port) of meta root directory
    hdfsFS _metaFs;                          //the hdfsFS of meta root directory
    list<SeisFileInfo> _file_list; //the list of all files that isn't expired but have been copied to nas
    bool _is_capacity_sufficient; //indicate whether the capacity of hdfs is sufficient
    double _seis_cap_ratio; //whether capacity of hdfs is sufficient bases on this parameter

    /**
     * list all the files and directories of a directory
     */
    void ListDirsFiles(string dir_path, vector<string>& dirList);

    /**
     * get the information of a node
     */
    void GetNodeInformation(SeisNode* &node, string path, SeisNode* parent);

    /**
     * get a child of a node by the parameter
     */
    SeisNode* GetChildByOrder(SeisNode* parent, int childOrder);

    /**
     * determine whether this file is expired or not
     */
    bool LifetimeExpired(string file_name, int lifetime, int64_t& surplus_lifetime);

    /**
     * determine whether hdfs capacity is sufficient
     */
    bool IsCapicitySufficient();

    /**
     * parse configure file
     */
    bool ParseParam();

public:

    /**
     * constructor
     */
    GarbageCollectorSeis();

    /**
     * destructor
     */
    virtual ~GarbageCollectorSeis();

    /**
     * post order traversal directory tree
     */
    bool PostOrderTraversal(double seis_cap_ratio);

    /**
     * remove the data already store in nas when hdfs used space ratio is too high
     */
    void Deduplication();
};

NAMESPACE_END_SEISFS
}
#endif /* GARBAGE_COLLECTOR_SEIS_H_ */
