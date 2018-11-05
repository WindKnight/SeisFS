/*
 * gcache_tmp_file.h
 *
 *  Created on: Dec 28, 2015
 *      Author: wyd
 */

#ifndef GCACHE_TMP_FILE_H_
#define GCACHE_TMP_FILE_H_

#include <stdint.h>
#include <sys/types.h>
#include <string>

#include "gcache_global.h"
#include "gcache_advice.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class Reader;
class Writer;

/*
 File是文件的基类，定义了文件类型能够提供的方法，包括读打开，写打开，文件大小，访问权限等。
 File是一个抽象类，无法实例化一个对象。我们为本地存储、集中存储和分布式存储分别定义了一个子类，定义了在不同存储类型上的方法的实现。
 通过调用File类的New方法，提供文件名和存储类型的参数，会生成相应的子类的对象，返回指向对象的指针，用户用指针对文件进行操作。
 */
class File {
public:

    virtual ~File() {
    }

    /*
     创建文件对象，根据s_arch区分文件是放在本地存储、集中存储还是分布式存储上。
     对用户来说，每个类型的存储都是一个大的“虚拟盘”，并不在乎底层存储介质。用户在虚拟盘上构建目录结构，读写文件。
     为了确定逻辑上的文件和实际存储位置的映射，需要为文件记录相应的元数据文件，其中记录了文件的实际存储介质，过期时间等信息。
     对每个存储类型来说，元数据都存放在固定的位置上。元数据按照与“虚拟盘”同样的目录结构进行组织，命名规则为：filename.meta
     如果分布式存储不存在的时候，需要自动变为集中存储
     */
    static File* New(const std::string &file_name, StorageArch &s_arch);

    /*
     通过OpenReader可以打开已存在的文件进行读操作，该方法会实例化一个Reader对象，返回指向该对象的指针，用户可以通过这个Reader对象的指针对文件进行读操作。
     该接口可以设置advice参数，可以选择FADV_RANDOM、FADV_SEQUENTIAL、FADV_RAW、FADV_MMAP等模式，对读操作进行优化。
     打开Reader首先会查询相应的元数据文件，如果元数据有效，则打开成功， 返回Reader的指针；如果元数据文件不存在或者文件已经过期无效，则打开失败，返回NULL。
     若打开成功，则可以得到文件的相应存储介质信息，从而解析出文件的物理存储路径，然后根据advice选用合适的vfs类型来进行文件操作。
     打开Reader不提供cache层级和过期时间的选项，文件的cache层级（即实际存储的物理介质）和过期时间在文件创建的时候决定。
     */
    virtual Reader *OpenReader(FAdvice advice = FADV_NONE) = 0;

    /*
     通过OpenWriter可以打开文件进行写操作，该方法会实例化一个Writer对象，返回指向该对象的指针，用户可以通过这个Writer对象的指针对文件进行写操作。
     该接口可以设置advice，对写操作进行优化。可以设置cache层级和过期时间的选项，并将这些选项记录到元数据中。
     如果OpenWiter的时候发现文件的元数据已经存在，这时需要进行判断：
     如果元数据信息指示文件已经过期，则删除过期的元数据文件和数据文件，建立新的元数据和数据文件；
     如果元数据信息指示文件依然有效，则保留原有的元数据，打开现有的文件进行写操作。

     利用OpenWriter打开文件之后，默认的模式是不清除原有的数据，并且将offset会默认设置到文件末尾位置，
     如果用户想要从其他位置开始写数据，需要自己调用Seek操作；如果用户想要清除原有数据，需要自己调用Truncate来进行清除。
     （如果默认清除数据，则无法支持随机写；如果默认从文件开头写，则HDFS默认的方式是清除数据，与上一条矛盾，会导致各种存储类型的语义不同）

     如果用户提供的文件名中的目录不存在的时候，需要自动建目录
     */
    virtual Writer *OpenWriter(lifetime_t lifetime_days = LIFETIME_DEFAULT, CacheLevel cache_lv =
            CACHE_L3, FAdvice advice = FADV_NONE) = 0;

//        virtual KVTable *OpenKVTable(lifetime_t lifetime_days =LIFETIME_DEFAULT, CompressAdvice cmp_adv = CMP_NON, CacheLevel cache_lv = CACHE_L3, FAdvice advice = FADV_NONE) = 0;

    virtual int64_t Size() const = 0;

    virtual bool Exists() const = 0;

    virtual lifetime_t Lifetime() const = 0;

    virtual bool SetLifetime(lifetime_t lifetime_days) = 0;

    virtual bool Remove() = 0;

    virtual bool Rename(const std::string &new_name) = 0;

    virtual bool Copy(const std::string &newName) = 0;

    virtual bool Copy(const std::string &newNanme, CacheLevel cache_lv) = 0;

    virtual bool Link(const std::string &link_name) = 0;

    virtual bool Truncate(int64_t size) = 0;

    virtual uint64_t LastModified() const = 0;

    virtual std::string GetName() const = 0;

    virtual bool LifetimeExpired() const = 0;

    virtual void Lock() = 0;

    virtual void Unlock() = 0;

protected:

    File() {
    }
    ;

private:

    virtual bool Init() = 0;
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_FILE_H_ */
