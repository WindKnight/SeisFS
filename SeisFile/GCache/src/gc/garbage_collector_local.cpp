/*
 * garbage_collector_local.cpp
 *
 *  Created on: Dec 12, 2016
 *      Author: node1
 */

#include "garbage_collector_local.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "seisfs_meta.h"
#include "gcache_tmp_meta.h"
#include "gcache_tmp_meta_local.h"

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

using namespace std;
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
GarbageCollectorLocal::GarbageCollectorLocal() {
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
GarbageCollectorLocal::~GarbageCollectorLocal() {
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
void GarbageCollectorLocal::ListDirsFiles(string dir_path, vector<string>& fileList,
        vector<string>& dirList) {
    if (-1 == access(dir_path.c_str(), F_OK)) {
        cout << "ERROR:list directory failed" << endl;
        return;
    }

    DIR *dir = opendir(dir_path.c_str());
    struct dirent entry;
    struct dirent *result;
    for (int iRet = readdir_r(dir, &entry, &result); iRet == 0 && result != NULL;
            iRet = readdir_r(dir, &entry, &result)) {

        if (0 == strcmp(entry.d_name, ".") || 0 == strcmp(entry.d_name, "..")) {
            continue;
        }

        string full_entry_name = dir_path + "/" + entry.d_name;
        struct stat stat_buf;
        int sRet = stat(full_entry_name.c_str(), &stat_buf);
        if (sRet < 0) {
            cout << "ERROR: STAT" << endl;
            closedir(dir);
            return;
        }

        if (S_ISREG(stat_buf.st_mode)) {
            fileList.push_back(full_entry_name);
        } else if (S_ISDIR(stat_buf.st_mode)) {
            dirList.push_back(full_entry_name);
        }
    }
    closedir(dir);
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
void GarbageCollectorLocal::GetNodeInformation(TreeNode* &node, string path, TreeNode* parent) {
    if (-1 == access(path.c_str(), 0)) {
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
 * @author              weibing
 * @version             0.1.4
 * @param               parent is target node, get the child_order child of target node.
 *                      child_order is the order of child.
 * @func                get a child of a node by the parameter.
 * @return              if the parent don't have the child_order child return NULL, else return the child_order child.
 */
TreeNode* GarbageCollectorLocal::GetChildByOrder(TreeNode* parent, int childOrder) {
    if (parent->DirectoryList.size() == (unsigned int) childOrder)    //the child isn't exist
            {
        return NULL;
    }

    TreeNode* child = NULL;
    GetNodeInformation(child, parent->DirectoryList[childOrder], parent); //get child information
    return child;
}

/**
 * LifetimeExpired
 *
 * @author              weibing
 * @version             0.1.4
 * @param               file_name is the name of a file.
 *                      lifetime is the life time of a file.
 * @func                determine whether this file is expired or not.
 * @return              if the file is expired return true, else return false.
 */
bool GarbageCollectorLocal::LifetimeExpired(string fileName, int lifetime) {
    struct timeval now;
    gettimeofday(&now, NULL);

    uint64_t time_now_ms = now.tv_sec * 1000 + now.tv_usec / 1000;
    struct stat f_stat;
    int iRet = stat(fileName.c_str(), &f_stat);
    if (-1 == iRet) {
        cout << "ERROR:stat failed" << endl;
        return 0;
    }
    uint64_t time_last_accessed_ms = f_stat.st_atim.tv_nsec / 1000000
            + f_stat.st_atim.tv_sec * 1000;
    uint64_t time_last_modified_ms = f_stat.st_mtim.tv_nsec / 1000000
            + f_stat.st_mtim.tv_sec * 1000;

    uint64_t time_last =
            time_last_accessed_ms > time_last_modified_ms ?
                    time_last_accessed_ms : time_last_modified_ms;

    uint64_t lifetime_ms = lifetime * 24 * 3600;
    lifetime_ms *= 1000;
    if (time_now_ms - time_last > lifetime_ms) {
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
bool GarbageCollectorLocal::ParseParam() {
    char *confile = getenv("GCACHE_TMP_LOCAL_CONF");
    if (confile == NULL) {
        cout << "ERROR:environment path GCACHE_TMP_LOCAL_CONF doesn't exist" << endl;
        return false;
    }
    string para_filename = confile;
    cout << "local para_filename = " << para_filename << endl;
    FILE *fp = fopen(para_filename.c_str(), "r");
    if (NULL == fp) {
        cout << "ERROR:Open file failed" << endl;
        return false;
    }

    char buf[1024];
    while (fgets(buf, 1024, fp) != NULL) {
        int len = strlen(buf);
        if (buf[len - 1] == '\n') buf[len - 1] = '\0';

        std::vector<std::string> params = SplitString(buf);

        int dirID = params[0][0];
        string dir = params[1] + "/";

        string entry_type = params[2];
        if (entry_type == "meta") {
            _meta_root_dir = dir;
        } else if (entry_type == "data") {
            _rootDirs.insert(make_pair(dirID, dir));
        }
    }

    fclose(fp);
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
bool GarbageCollectorLocal::PostOrderTraversal() {
    cout << "local garbage collection start" << endl;

    if (!ParseParam()) {
        cout << "ERROR:ParseParam failed" << endl;
        return false;
    }
    string rootPath = _meta_root_dir;
    TreeNode* rootNode = NULL;
    GetNodeInformation(rootNode, rootPath, NULL); //get information of root node
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
            if (rootNode->DirectoryList.size() == (unsigned int) rootNode->CurrentChildOrder) //all the directory in current node have been traversed
                    {
                //  cout<<rootNode->NodePath<<endl;  //print the current node

                for (unsigned int i = 0; i < rootNode->FileList.size(); i++) {

                    int read_len = sizeof(MetaTMPLocal);
                    MetaTMPLocal* metaData = new MetaTMPLocal();

                    int fd = open(rootNode->FileList[i].c_str(), O_RDONLY);
                    if (fd == -1) {
                        cout << "ERROR:open file failed" << endl;
                        delete metaData;
                        continue;
                    }

                    char sizeBuf[4];
                    int size_len = 4;
                    if (size_len != read(fd, sizeBuf, size_len)) {
                        delete metaData;
                        close(fd);
                        cout << "ERROR:read file failed" << endl;
                        continue;
                    }

                    if (read_len != read(fd, metaData, read_len)) {
                        delete metaData;
                        close(fd);
                        cout << "ERROR:read file failed" << endl;
                        continue;
                    }
                    close(fd);

                    int dir_id = metaData->meta.dirID;
                    string root_dir = _rootDirs[dir_id];
                    string sub_filename = rootNode->FileList[i].substr(_meta_root_dir.length(),
                            rootNode->FileList[i].length() - _meta_root_dir.length());
                    string real_fileName = root_dir + "/" + sub_filename;

                    if (!LifetimeExpired(real_fileName, metaData->meta.header.lifetime)) {
                        delete metaData;
                        continue;
                    }

                    struct stat buf;
                    if (lstat(real_fileName.c_str(), &buf) < 0) {
                        cout << "ERROR:lstat" << endl;
                        delete metaData;
                        continue;
                    }
                    int is_dir = __S_IFDIR & buf.st_mode;
                    if (remove(rootNode->FileList[i].c_str()) == 0) {
                        rootNode->RemovedFileNumber++;
                        if (is_dir) {
                            RecursiveRemove(real_fileName);
                        } else {
                            remove(real_fileName.c_str());
                        }
                    }
                    delete metaData;
                }

                if (nodeStack.size() == 1) {
                    TreeNode* top = nodeStack.top();
                    nodeStack.pop();
                    delete top;
                    break;
                }

                string subDir = rootNode->NodePath.substr(_meta_root_dir.length(),
                        rootNode->NodePath.length() - _meta_root_dir.length());

                if ((unsigned int) rootNode->RemovedFileNumber == rootNode->FileList.size()
                        && (unsigned int) rootNode->RemovedDirectoryNumber
                                == rootNode->DirectoryList.size()) //the files and directories in current node are removed
                                {
                    if (remove(rootNode->NodePath.c_str()) == 0) {

                        for (map<int, string>::iterator it = _rootDirs.begin();
                                it != _rootDirs.end(); ++it) {
                            string full_dir_path = it->second + subDir;
                            remove(full_dir_path.c_str());
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
                rootNode = GetChildByOrder(rootNode, rootNode->CurrentChildOrder);  //get next child
            }
        }
    } while (nodeStack.size() > 0);

    return true;
}

NAMESPACE_END_SEISFS
}
