#ifndef  MDT_UTIL_SORT_BLOCK_H_
#define  MDT_UTIL_SORT_BLOCK_H_

#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "table/block.h"

#if 0
#include <map>
#include <string>
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"
#endif

namespace mdt {

class BlockBuilder {
public:
    BlockBuilder(::leveldb::Options* options);
    ~BlockBuilder();
    void Add(const ::leveldb::Slice& user_key, const ::leveldb::Slice& user_value);
    uint64_t NumEntries();
    void Finish();
    ::leveldb::Slice GetCompressionBlock();

private:
    void Reset();

private:
    // primary key comparator
    ::leveldb::Options* options_;

    // construct memtable, memtable operator: ::leveldb::WriteBatch, ::leveldb::WriteBatchInternal
    uint64_t sequence_num_;
    ::leveldb::InternalKeyComparator internal_comparator_;
    // user key => user key + seq + type, value = user value
    ::leveldb::MemTable* memtable_;

    // construct block with prefix key compress + snappy compress
    ::leveldb::BlockBuilder data_block_;
    std::string compressed_output_;
};

class IndexBlock {
public:
    static ::leveldb::Status ConstructBlockContents(const ::leveldb::Slice& raw, ::leveldb::BlockContents* contents,
                                                    const ::leveldb::ReadOptions& options);
    explicit IndexBlock(::leveldb::BlockContents& contents, ::leveldb::Options options);
    ~IndexBlock();
    // convert internal leveldb key into user key iter
    ::leveldb::Iterator* NewIterator();

private:
    ::leveldb::Options options_;
    ::leveldb::Block* block_;
};

}  // namespace mdt

#endif
