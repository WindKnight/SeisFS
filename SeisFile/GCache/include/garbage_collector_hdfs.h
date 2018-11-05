/*
 * garbage_collector_hdfs.h
 *
 *  Created on: Dec 19, 2016
 *      Author: node1
 */

#ifndef GARBAGE_COLLECTOR_HDFS_H_
#define GARBAGE_COLLECTOR_HDFS_H_

#include <map>
#include <string>
#include <vector>
#include "tree_node.h"

using namespace std;

NAMESPACE_BEGIN_SEISFS

namespace tmp {
/**
 * this class provides some function of expired hdfs files remove.
 */
class GarbageCollectorHdfs {
private:
    map<int, HdfsFilePathInfo> _rootDirs; //used for recording the hdfs id, pure path and hdfsFS of a file
    string _meta_root_dir; //the pure path(contains no ip address and port) of meta root directory
    hdfsFS _metaFs;                          //the hdfsFS of meta root directory

    /**
     * list all the files and directories of a directory
     */
    void ListDirsFiles(string dir_path, vector<string>& fileList, vector<string>& dirList);

    /**
     * get the information of a node
     */
    void GetNodeInformation(TreeNode* &node, string path, TreeNode* parent);

    /**
     * get a child of a node by the parameter
     */
    TreeNode* GetChildByOrder(TreeNode* parent, int childOrder);

    /**
     * determine whether this file is expired or not
     */
    bool LifetimeExpired(hdfsFS fileFs, string file_name, int lifetime);

    /**
     * parse configure file
     */
    bool ParseParam();

public:

    /**
     * constructor
     */
    GarbageCollectorHdfs();

    /**
     * destructor
     */
    virtual ~GarbageCollectorHdfs();

    /**
     * post order traversal directory tree
     */
    bool PostOrderTraversal();
};

NAMESPACE_END_SEISFS
}
#endif /* GARBAGE_COLLECTOR_HDFS_H_ */
