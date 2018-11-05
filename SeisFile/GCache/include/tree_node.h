/*
 * tree_node.h
 *
 *  Created on: Dec 12, 2016
 *      Author: node1
 */

#ifndef TREE_NODE_H_
#define TREE_NODE_H_

#include "hdfs.h"
#include <vector>
#include "gcache_global.h"

using namespace std;

NAMESPACE_BEGIN_SEISFS

struct TreeNode {
public:

    string NodePath;        //the path of current node

    vector<string> DirectoryList;  //the sub-directories of current node

    vector<string> FileList;    //the files of current node

    int CurrentChildOrder; //the sequence number of the current retrieved child node, starting at 0, and the leftmost child node number is 0

    TreeNode* ParentNode;  //the parent of current node

    int RemovedFileNumber; //the number of files in the current node that have been deleted

    int RemovedDirectoryNumber; //the number of directories in the current node that have been deleted

};

struct SeisNode {
public:

    string NodePath;  //the path of current node

    vector<string> DirectoryList; //the collection of child nodes (directories) contained in the current node

    int CurrentChildOrder; //the sequence number of the current retrieved child node, starting at 0, and the leftmost child node number is 0

    int RemovedDirectoryNumber; //the number of directories in the current node that have been deleted

    SeisNode* ParentNode;  //the parent of current node

    bool RemovedFlag;  //marks whether the node is to be deleted

    bool IsSeisData;  //marks whether the node is SeisData

};

struct HdfsFilePathInfo {
public:
    string PurePath;       //file path in HDFS

    hdfsFS Fs;             //hdfsFS
};

struct SeisFileInfo {
public:
    string PurePath;       //file path in HDFS

    int64_t SurplusLifetime;   //the remaining life of the file, in milliseconds

    bool operator <(SeisFileInfo& element) {
        return SurplusLifetime < element.SurplusLifetime;
    }
};

NAMESPACE_END_SEISFS

#endif /* TREE_NODE_H_ */
