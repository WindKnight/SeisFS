/*
 * garbage_collector_seis.cpp
 *
 *  Created on: Dec 19, 2016
 *      Author: node1
 */

#include "garbage_collector_seis.h"

#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_meta.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iostream>
#include <stack>
#include <utility>

NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * constructor
 *
 * @author                  weibing
 * @version                0.1.4
 * @param
 * @func                construct a object
 * @return                no return
 */
GarbageCollectorSeis::GarbageCollectorSeis() {

}

/**
 * destructor
 *
 * @author              weibing
 * @version             0.1.4
 * @param
 * @func                destruct a object
 * @return              no return
 */
GarbageCollectorSeis::~GarbageCollectorSeis() {
}

/**
 * ListDirsFiles
 *
 * @author              weibing
 * @version             0.1.4
 * @param               dir_path is the path of a node.
 *                      fileList is the file list of a node.
 *                      dirList is the directory list of a node.
 * @func                list all the files and directories of a directory.
 * @return              no return.
 */
void GarbageCollectorSeis::ListDirsFiles(string dir_path, vector<string>& dirList) {
    int count = 0;
    hdfsFileInfo * listFileInfo = hdfsListDirectory(_metaFs, dir_path.c_str(), &count);
    if (listFileInfo == NULL) {
        cout << "ERROR:list directory failed" << endl;
        return;
    }

    for (int i = 0; i < count; ++i) {
        if (listFileInfo[i].mKind == 'D') {
            dirList.push_back(listFileInfo[i].mName);
        }
    }
    hdfsFreeFileInfo(listFileInfo, count);
}

/**
 * GetNodeInformation
 *
 * @author              weibing
 * @version             0.1.4
 * @param               node is a target node, the node will contain all the information after the function.
 *                      path is the path of target node.
 *                      parent is the parent of target node.
 * @func                get the information of a node.
 * @return              no return.
 */
void GarbageCollectorSeis::GetNodeInformation(SeisNode* &node, string path, SeisNode* parent) {
    if (hdfsExists(_metaFs, path.c_str()) == -1) {
        return;
    }

    node = new SeisNode();
    node->NodePath = path;
    node->RemovedFlag = false;
    node->ParentNode = parent;
    node->CurrentChildOrder = 0;                 //the order of first child is 0
    node->RemovedDirectoryNumber = 0; //refer to the number of removed directories in node

    string real_meta_name = path + "/" + META_NAME;
    if (hdfsExists(_metaFs, real_meta_name.c_str()) == 0) {
        node->IsSeisData = true;
        hdfsFile fd = hdfsOpenFile(_metaFs, real_meta_name.c_str(), O_RDONLY, 0, 0, 0);
        if (NULL == fd) {
            cout << "ERROR:open file failed" << endl;
            return;
        }

        int read_len = sizeof(MetaSeisHDFS);
        MetaSeisHDFS* metaData = new MetaSeisHDFS();
        if (read_len != hdfsRead(_metaFs, fd, metaData, read_len)) {
            delete metaData;
            hdfsCloseFile(_metaFs, fd);
            cout << "ERROR:read file failed" << endl;
            return;
        }

        int64_t surplus_lifetime;
        if (LifetimeExpired(path, metaData->meta.lifetime, surplus_lifetime)) {
            node->RemovedFlag = true;
        } else if (!_is_capacity_sufficient && metaData->meta.tag == MOVE_TO_NAS) {
            SeisFileInfo survive_file;
            survive_file.PurePath = node->NodePath;
            survive_file.SurplusLifetime = surplus_lifetime;
            _file_list.push_back(survive_file);
        }

        hdfsCloseFile(_metaFs, fd);
    } else {
        node->IsSeisData = false;
        ListDirsFiles(path, node->DirectoryList); //get all the directories in node
    }

}

/**
 * GetChildByOrder
 *
 * @author              weibing
 * @version             0.1.4
 * @param               parent is target node, get the child_order child of target node.
 *                      child_order is the order of child.
 * @func                get a child of a node by the parameter.
 * @return              if the parent don't have the child_order child return NULL, else return the child_order child.
 */
SeisNode* GarbageCollectorSeis::GetChildByOrder(SeisNode* parent, int child_order) {
    if (parent->DirectoryList.size() == (unsigned int) child_order) {
        return NULL;
    }

    SeisNode* child = NULL;
    GetNodeInformation(child, parent->DirectoryList[child_order], parent);
    return child;
}

/**
 * LifetimeExpired
 *
 * @author              weibing
 * @version             0.1.4
 * @param               fileFs is the hdfsFS of a file in hdfs.
 *                      file_name is the name of a file.
 *                      lifetime is the life time of a file.
 *                      surplus_lifetime is the surplus lifetime of a file.
 * @func                determine whether this file is expired or not.
 * @return              if the file is expired return true, else return false.
 */
bool GarbageCollectorSeis::LifetimeExpired(string file_name, int lifetime,
        int64_t& surplus_lifetime) {
    struct timeval now;
    gettimeofday(&now, NULL);

    int64_t time_now_ms = now.tv_sec * 1000 + now.tv_usec / 1000;
    hdfsFileInfo * fileInfo = hdfsGetPathInfo(_metaFs, file_name.c_str());
    if (fileInfo == NULL) {
        cout << "ERROR:file stat failed" << endl;
        return false;
    }
    int64_t time_last_accessed_ms =
            fileInfo->mLastAccess > fileInfo->mLastMod ?
                    fileInfo->mLastAccess * 1000 : fileInfo->mLastMod * 1000;

    hdfsFreeFileInfo(fileInfo, 1);

    int64_t lifetime_ms = lifetime * 24 * 3600;
    lifetime_ms *= 1000;
    int64_t timespan = time_now_ms - time_last_accessed_ms;

    if (timespan > lifetime_ms) {
        return true;
    } else {
        surplus_lifetime = lifetime_ms - timespan;
        return false;
    }
}

/**
 * IsCapicitySufficient
 *
 * @author              weibing
 * @version             0.1.4
 * @param
 * @func                determine whether hdfs capacity is sufficient.
 * @return              if sufficient return true,else return false;
 */
bool GarbageCollectorSeis::IsCapicitySufficient() {
    double total_space = hdfsGetCapacity(_metaFs);
    if (total_space == -1) {
        cout << "ERROR:get hdfs total capacity failed" << endl;
        return true;
    }

    double used_space = hdfsGetUsed(_metaFs);
    if (used_space == -1) {
        cout << "ERROR:get hdfs used capacity failed" << endl;
        return true;
    }
    double used_ratio = used_space / total_space;
//    double used_ratio = 1;
    if (used_ratio > _seis_cap_ratio) {
        return false;
    } else {
        return true;
    }
}

/**
 * ParseParam
 *
 * @author              weibing
 * @version             0.1.4
 * @param
 * @func                parse configure file.
 * @return              if parse param successfully return true, else return false.
 */
bool GarbageCollectorSeis::ParseParam() {

    char *seisRootDir = getenv("SEIS_ROOT_DIR");
    if (seisRootDir == NULL) {
        cout << "ERROR:envionment variable SEIS_ROOT_DIR is NULL" << endl;
        return false;
    }

//    char *seisCapRatio = getenv("SEIS_CAPACITY_RATIO");
//    if (seisCapRatio == NULL) {
//        cout << "ERROR:envionment variable SEIS_CAPACITY_RATIO is NULL" << endl;
//        return false;
//    }

//    _seis_cap_ratio = ToDouble(seisCapRatio);

    string param_path = seisRootDir;

    _meta_root_dir = hdfsGetPath(param_path);
    _metaFs = getHDFS(param_path);

    return true;
}

/**
 * PostOrderTraversal
 *
 * @author              weibing
 * @version             0.1.4
 * @param
 * @func                post order traversal directory tree.
 * @return              if traversal successfully return true, else return false.
 */
bool GarbageCollectorSeis::PostOrderTraversal(double seis_cap_ratio) {
    cout << "seis garbage collection start" << endl;

    if (seis_cap_ratio <= 0) {
        cout << "ERROR:set seis_cap_ratio failed, seis_cap_ratio<=0" << endl;
        return false;
    } else if (seis_cap_ratio > 1) {
        seis_cap_ratio = 1;
    }
    _seis_cap_ratio = seis_cap_ratio;

    if (!ParseParam()) {
        cout << "ERROR:ParseParam failed" << endl;
        return false;
    }

    _is_capacity_sufficient = IsCapicitySufficient();

    string root_path = _meta_root_dir;
    SeisNode* rootNode = NULL;
    GetNodeInformation(rootNode, root_path, NULL); //get information of root node
    stack<SeisNode*> nodeStack;                //used for no recursive traversal

    if (rootNode == NULL) {
        cout << "ERROR:the root path doesn't exist" << endl;
        return false;
    }

    bool flag = true;
    do {
        while (rootNode != NULL) {
            nodeStack.push(rootNode);             //push current node into stack
            rootNode = GetChildByOrder(rootNode, 0);       //get the first child
        }

        flag = true;
        while (nodeStack.size() > 0 && flag) {
            rootNode = nodeStack.top();           //get the element of top stack
            if (rootNode->DirectoryList.size() == (unsigned int) rootNode->CurrentChildOrder) //traverse all the directory in current node
                    {
                //  cout<<rootNode->NodePath<<endl;   //print the current node

                if (nodeStack.size() == 1) {
                    SeisNode* top = nodeStack.top();
                    nodeStack.pop();
                    delete top;
                    continue;
                }

                if (rootNode->RemovedFlag) {
                    if (hdfsDelete(_metaFs, rootNode->NodePath.c_str(), 1) == 0) {
                        rootNode->ParentNode->RemovedDirectoryNumber++;
                    }
                }

                if (!rootNode->IsSeisData
                        && (unsigned int) rootNode->RemovedDirectoryNumber
                                == rootNode->DirectoryList.size()) //the files and directories in current node are removed
                                {
                    if (hdfsDelete(_metaFs, rootNode->NodePath.c_str(), 0) == 0) {
                        rootNode->ParentNode->RemovedDirectoryNumber++;
                    }
                }

                SeisNode* top = nodeStack.top();
                nodeStack.pop();
                delete top;
            } else                      //at least one sub-node is not traversed
            {
                flag = false;
                rootNode->CurrentChildOrder++; //calculate the next sub-node order
                rootNode = GetChildByOrder(rootNode, rootNode->CurrentChildOrder);
            }
        }
    } while (nodeStack.size() > 0);

    Deduplication();
    hdfsDisconnect(_metaFs);

    return true;
}

/**
 * Deduplication
 *
 * @author              weibing
 * @version             0.1.4
 * @param
 * @func                remove the data already store in nas when hdfs used space ratio is too high.
 * @return
 */
void GarbageCollectorSeis::Deduplication() {
    _file_list.sort();
    bool is_capacity_sufficient = IsCapicitySufficient();
    while (!is_capacity_sufficient && _file_list.size() > 0) {
        SeisFileInfo front_element = _file_list.front();
        hdfsDelete(_metaFs, front_element.PurePath.c_str(), 1);

        int pos = front_element.PurePath.find_last_of("/");
        string parent_path = front_element.PurePath.substr(0, pos);

        while (parent_path.length() > _meta_root_dir.length()) {

            int count = 0;
            hdfsFileInfo * listFileInfo = hdfsListDirectory(_metaFs, parent_path.c_str(), &count);
            if (listFileInfo == NULL) {
                cout << "ERROR:list directory failed" << endl;
                break;
            }

            if (count != 0) {
                hdfsFreeFileInfo(listFileInfo, count);
                break;
            } else {
                hdfsDelete(_metaFs, parent_path.c_str(), 0);
                pos = parent_path.find_last_of("/");
                string temp = parent_path.substr(0, pos);
                parent_path = temp;
            }

        }

        _file_list.pop_front();
        is_capacity_sufficient = IsCapicitySufficient();
    }

}

NAMESPACE_END_SEISFS
}
