/*
 * garbage_collector_hdfs.cpp
 *
 *  Created on: Dec 19, 2016
 *      Author: node1
 */

#include "garbage_collector_hdfs.h"

#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_hdfs_utils.h"
#include "seisfs_meta.h"
#include "gcache_tmp_meta.h"
#include "gcache_tmp_meta_hdfs.h"

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
namespace tmp {
/**
 * constructor
 *
 * @author              weibing
 * @version             0.1.4
 * @param
 * @func                construct a object
 * @return              no return
 */
GarbageCollectorHdfs::GarbageCollectorHdfs() {
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
GarbageCollectorHdfs::~GarbageCollectorHdfs() {
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
void GarbageCollectorHdfs::ListDirsFiles(string dir_path, vector<string>& fileList,
        vector<string>& dirList) {
    int count = 0;
    hdfsFileInfo * listFileInfo = hdfsListDirectory(_metaFs, dir_path.c_str(), &count);
    if (listFileInfo == NULL) {
        cout << "ERROR:list directory failed" << endl;
        return;
    }

    for (int i = 0; i < count; ++i) {
        if (listFileInfo[i].mKind == 'D') {
            dirList.push_back(listFileInfo[i].mName);
        } else if (listFileInfo[i].mKind == 'F') {
            fileList.push_back(listFileInfo[i].mName);
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
void GarbageCollectorHdfs::GetNodeInformation(TreeNode* &node, string path, TreeNode* parent) {
    if (hdfsExists(_metaFs, path.c_str()) == -1) {
        return;
    }

    node = new TreeNode();
    node->NodePath = path;
    node->CurrentChildOrder = 0;                 //the order of first child is 0
    node->ParentNode = parent;
    node->RemovedDirectoryNumber = 0; //refer to the number of removed directories in node
    node->RemovedFileNumber = 0;  //refer to the number of removed files in node

    ListDirsFiles(path, node->FileList, node->DirectoryList); //get all the files and directories in node

}

/**
 * GetChildByOrder
 *
 * @author               weibing
 * @version              0.1.4
 * @param                parent is target node, get the child_order child of target node.
 *                       child_order is the order of child.
 * @func                 get a child of a node by the parameter.
 * @return               if the parent don't have the child_order child return NULL, else return the child_order child.
 */
TreeNode* GarbageCollectorHdfs::GetChildByOrder(TreeNode* parent, int child_order) {
    if (parent->DirectoryList.size() == (unsigned int) child_order) {
        return NULL;
    }

    TreeNode* child = NULL;
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
 * @func                determine whether this file is expired or not.
 * @return              if the file is expired return true, else return false.
 */
bool GarbageCollectorHdfs::LifetimeExpired(hdfsFS fileFs, string file_name, int lifetime) {
    struct timeval now;
    gettimeofday(&now, NULL);

    int64_t time_now_ms = now.tv_sec * 1000 + now.tv_usec / 1000;
    hdfsFileInfo * fileInfo = hdfsGetPathInfo(fileFs, file_name.c_str());
    if (fileInfo == NULL) {
        cout << "ERROR:file stat failed" << endl;
        return false;
    }
    int64_t time_last_accessed_ms = fileInfo->mLastAccess * 1000;
    hdfsFreeFileInfo(fileInfo, 1);

    int64_t lifetime_ms = lifetime * 24 * 3600;
    lifetime_ms *= 1000;
    int64_t timespan = time_now_ms - time_last_accessed_ms;
    if (timespan > lifetime_ms) {
        return true;
    } else {
        return false;
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
bool GarbageCollectorHdfs::ParseParam() {
    char *confile = getenv("GCACHE_TMP_HDFS_CONF");
    if (confile == NULL) {
        cout << "ERROR:environment path GCACHE_TMP_HDFS_CONF doesn't exist" << endl;
        return false;
    }
    string para_filename = confile;
    cout << "hdfs para_filename = " << para_filename << endl;

    if (para_filename.find("hdfs://") != (unsigned int) -1) //parafile in hdfs
            {
        string path = hdfsGetPath(para_filename);
        if (!StartsWith(path, "/")) {
            path = "/" + path;
        }
        hdfsFS paraFs = getHDFS(para_filename);
        hdfsFile fd = hdfsOpenFile(paraFs, path.c_str(), O_RDONLY, 0, 0, 0);
        if (NULL == fd) {
            cout << "ERROR:File open failed" << endl;
            hdfsDisconnect(paraFs);
            return false;
        }

        char buf[1024];
        while (hdfsFgets(buf, 1024, paraFs, fd) != NULL) {
            int len = strlen(buf);
            if (buf[len - 1] == '\n') {
                buf[len - 1] = '\0';
            }

            vector<string> params = SplitString(buf);
            int dirID = params[0][0];
            string whole_path = params[1] + "/";
            hdfsFS fs = getHDFS(whole_path);
            string rootDir = hdfsGetPath(whole_path);
            string entry_type = params[2];

            if (entry_type == "meta") {
                _meta_root_dir = rootDir;
                _metaFs = fs;
            } else if (entry_type == "data") {
                HdfsFilePathInfo info;
                info.Fs = fs;
                info.PurePath = rootDir;
                _rootDirs.insert(make_pair(dirID, info));
            }
        }
        hdfsCloseFile(paraFs, fd);
        hdfsDisconnect(paraFs);
    } else //parafile in nas
    {
        FILE *fp = fopen(para_filename.c_str(), "r");
        if (NULL == fp) {
            cout << "ERROR:Open file failed" << endl;
            return false;
        }

        char buf[1024];
        while (fgets(buf, 1024, fp) != NULL) {
            int len = strlen(buf);
            if (buf[len - 1] == '\n') {
                buf[len - 1] = '\0';
            }

            vector<string> params = SplitString(buf);
            int dirID = params[0][0];
            string whole_path = params[1] + "/";
            hdfsFS fs = getHDFS(whole_path);
            string rootDir = hdfsGetPath(whole_path);
            string entry_type = params[2];

            if (entry_type == "meta") {
                _meta_root_dir = rootDir;
                _metaFs = fs;
            } else if (entry_type == "data") {
                HdfsFilePathInfo info;
                info.Fs = fs;
                info.PurePath = rootDir;
                _rootDirs.insert(make_pair(dirID, info));
            }
        }

        fclose(fp);

    }

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
bool GarbageCollectorHdfs::PostOrderTraversal() {
    cout << "hdfs garbage collection start" << endl;

    if (!ParseParam()) {
        cout << "ERROR:ParseParam failed" << endl;
        return false;
    }

    string root_path = _meta_root_dir;
    TreeNode* rootNode = NULL;
    GetNodeInformation(rootNode, root_path, NULL); //get information of root node
    stack<TreeNode*> nodeStack;                //used for no recursive traversal

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
                //        cout<<rootNode->NodePath<<endl;   //print the current node

                for (unsigned int i = 0; i < rootNode->FileList.size(); i++) {
                    int read_len = sizeof(MetaTMPHDFS);
                    MetaTMPHDFS* metaData = new MetaTMPHDFS();

                    hdfsFile fd = hdfsOpenFile(_metaFs, rootNode->FileList[i].c_str(), O_RDONLY, 0,
                            1, 0);
                    if (fd == NULL) {
                        cout << "ERROR:open file failed" << endl;
                        delete metaData;
                        continue;
                    }

                    char sizeBuf[4];
                    int size_len = 4;
                    if (size_len != hdfsRead(_metaFs, fd, sizeBuf, size_len)) {
                        delete metaData;
                        hdfsCloseFile(_metaFs, fd);
                        cout << "ERROR:read file failed" << endl;
                        continue;
                    }

                    if (read_len != hdfsRead(_metaFs, fd, metaData, read_len)) {
                        delete metaData;
                        hdfsCloseFile(_metaFs, fd);
                        cout << "ERROR:read file failed" << endl;
                        continue;
                    }
                    hdfsCloseFile(_metaFs, fd);

                    int dir_id = metaData->meta.dirID;
                    hdfsFS fileFs = _rootDirs[dir_id].Fs;
                    string root_dir = _rootDirs[dir_id].PurePath;
                    string sub_filename = rootNode->FileList[i].substr(_meta_root_dir.length(),
                            rootNode->FileList[i].length() - _meta_root_dir.length());
                    string real_filename = root_dir + "/" + sub_filename;

                    if (!LifetimeExpired(fileFs, real_filename, metaData->meta.header.lifetime)) {
                        delete metaData;
                        continue;
                    }

                    if (hdfsDelete(_metaFs, rootNode->FileList[i].c_str(), 0) == 0) {
                        rootNode->RemovedFileNumber++;
                        hdfsDelete(fileFs, real_filename.c_str(), 1);
                    }
                    delete metaData;
                }

                if (nodeStack.size() == 1) {
                    TreeNode* top = nodeStack.top();
                    nodeStack.pop();
                    delete top;
                    continue;
                }

                string subDir = rootNode->NodePath.substr(_meta_root_dir.length(),
                        rootNode->NodePath.length() - _meta_root_dir.length());
                if ((unsigned int) rootNode->RemovedFileNumber == rootNode->FileList.size()
                        && (unsigned int) rootNode->RemovedDirectoryNumber
                                == rootNode->DirectoryList.size()) //the files and directories in current node are removed
                                {
                    if (hdfsDelete(_metaFs, rootNode->NodePath.c_str(), 0) == 0) {
                        for (map<int, HdfsFilePathInfo>::iterator it = _rootDirs.begin();
                                it != _rootDirs.end(); ++it) //remove the same directory in other hdfs
                                {
                            hdfsFS fs = it->second.Fs;
                            string real_dir_path = it->second.PurePath + subDir;
                            hdfsDelete(fs, real_dir_path.c_str(), 0);
                        }
                        rootNode->ParentNode->RemovedDirectoryNumber++;
                    }
                }

                TreeNode* top = nodeStack.top();
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

    hdfsDisconnect(_metaFs);
    for (map<int, HdfsFilePathInfo>::iterator it = _rootDirs.begin(); it != _rootDirs.end(); ++it) {
        hdfsDisconnect(it->second.Fs);
    }

    return true;
}

NAMESPACE_END_SEISFS
}
