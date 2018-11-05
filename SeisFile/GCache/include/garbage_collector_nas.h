/*
 * garbage_collector_nas.h
 *
 *  Created on: Dec 12, 2016
 *      Author: node1
 */

#ifndef GARBAGE_COLLECTOR_NAS_H_
#define GARBAGE_COLLECTOR_NAS_H_

#include <map>
#include <string>
#include <vector>
#include "tree_node.h"

using namespace std;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * this class provides some function of expired nas files remove.
 */
class GarbageCollectorNas {

private:
    map<int, string> _rootDirs; //used for recording the disk id and path of a file
    string _meta_root_dir;        //the path of meta root directory

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
    bool LifetimeExpired(string fileName, int lifetime);

    /**
     * parse configure file
     */
    bool ParseParam();

public:

    /**
     * constructor
     */
    GarbageCollectorNas();

    /**
     * destructor
     */
    virtual ~GarbageCollectorNas();

    /**
     * post order traversal directory tree
     */
    bool PostOrderTraversal();
};

NAMESPACE_END_SEISFS
}
#endif /* GARBAGE_COLLECTOR_NAS_H_ */
