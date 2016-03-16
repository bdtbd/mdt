#include "utils/sort_block.h"
#include <glog/logging.h>
#include "util/crc32c.h"

#if 0
#include "leveldb/table.h"

#include <map>
#include <string>
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"
#endif

namespace mdt {

////////////////////////////////////////////////
// sort block builder
////////////////////////////////////////////////
BlockBuilder::BlockBuilder(::leveldb::Options* options)
    : options_(options),
    internal_comparator_(options_->comparator),
    data_block_(options_) {

    sequence_num_ = 0;
    memtable_ = new ::leveldb::MemTable(internal_comparator_);
    memtable_->Ref();
}

BlockBuilder::~BlockBuilder() {
    memtable_->Unref();
}

void BlockBuilder::Reset() {
    memtable_->Unref();
    compressed_output_.clear();

    sequence_num_ = 0;
    memtable_ = new ::leveldb::MemTable(internal_comparator_);
    memtable_->Ref();
}

void BlockBuilder::Add(const ::leveldb::Slice& user_key, const ::leveldb::Slice& user_value) {
    sequence_num_++;
    memtable_->Add(sequence_num_, ::leveldb::kTypeValue, user_key, user_value);
}

uint64_t BlockBuilder::NumEntries() {
    return sequence_num_;
}

void BlockBuilder::Finish() {
    // store user_key+seq+type, user_value into sort block
    ::leveldb::Iterator* iter = memtable_->NewIterator();
    iter->SeekToFirst();
    while (iter->Valid()) {
        // key prefix compress
        data_block_.Add(iter->key(), iter->value());
        iter->Next();
    }
    delete iter;

    // data compress and crc
    ::leveldb::Slice raw = data_block_.Finish();

    ::leveldb::Slice block_contents;
    ::leveldb::CompressionType type = options_->compression;
    // TODO(postrelease): Support more compression options: zlib?
    switch (type) {
        case ::leveldb::kNoCompression:
            block_contents = raw;
            break;

        case ::leveldb::kSnappyCompression: {
            std::string* compressed = &compressed_output_;
            if (::leveldb::port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) {
                block_contents = *compressed;
            } else {
                // Snappy not supported, or compressed less than 12.5%, so just
                // store uncompressed form
                block_contents = raw;
                type = ::leveldb::kNoCompression;
            }
            break;
        }
    }
    VLOG(30) << "compress block, row size " << raw.size() << ", compress size " << block_contents.size();

    // use crc
    char trailer[::leveldb::kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = ::leveldb::crc32c::Value(block_contents.data(), block_contents.size());
    crc = ::leveldb::crc32c::Extend(crc, trailer, 1);
    ::leveldb::EncodeFixed32(trailer + 1, ::leveldb::crc32c::Mask(crc));
    compressed_output_.append(trailer, ::leveldb::kBlockTrailerSize);

    // do some cleanup
    data_block_.Reset();
    //Reset();
    return;
}

::leveldb::Slice BlockBuilder::GetCompressionBlock() {
    return ::leveldb::Slice(compressed_output_);
}

///////////////////////////////////////////////
// IndexBlock, parse data content into kv map
///////////////////////////////////////////////
// A helper class that converts internal format keys into user keys
class KeyConvertingIterator: public ::leveldb::Iterator {
 public:
  explicit KeyConvertingIterator(::leveldb::Iterator* iter) : iter_(iter) { }
  virtual ~KeyConvertingIterator() { delete iter_; }
  virtual bool Valid() const { return iter_->Valid(); }
  virtual void Seek(const ::leveldb::Slice& target) {
    ::leveldb::ParsedInternalKey ikey(target, ::leveldb::kMaxSequenceNumber, ::leveldb::kTypeValue);
    std::string encoded;
    ::leveldb::AppendInternalKey(&encoded, ikey);
    iter_->Seek(encoded);
  }
  virtual void SeekToFirst() { iter_->SeekToFirst(); }
  virtual void SeekToLast() { iter_->SeekToLast(); }
  virtual void Next() { iter_->Next(); }
  virtual void Prev() { iter_->Prev(); }

  virtual ::leveldb::Slice key() const {
    assert(Valid());
    ::leveldb::ParsedInternalKey key;
    if (!::leveldb::ParseInternalKey(iter_->key(), &key)) {
      status_ = ::leveldb::Status::Corruption("malformed internal key");
      return ::leveldb::Slice("corrupted key");
    }
    return key.user_key;
  }

  virtual ::leveldb::Slice value() const { return iter_->value(); }
  virtual ::leveldb::Status status() const {
    return status_.ok() ? iter_->status() : status_;
  }

 private:
  mutable ::leveldb::Status status_;
  ::leveldb::Iterator* iter_;

  // No copying allowed
  KeyConvertingIterator(const KeyConvertingIterator&);
  void operator=(const KeyConvertingIterator&);
};

::leveldb::Status IndexBlock::ConstructBlockContents(const ::leveldb::Slice& raw, ::leveldb::BlockContents* contents,
                                                     const ::leveldb::ReadOptions& options) {
    if (raw.size() < ::leveldb::kBlockTrailerSize) {
        return ::leveldb::Status::Corruption("truncate block read");
    }
    const char* data = raw.data();
    if (options.verify_checksums) {
        const uint32_t crc = ::leveldb::crc32c::Unmask(::leveldb::DecodeFixed32(data + raw.size() - ::leveldb::kBlockTrailerSize + 1));
        const uint32_t actual = ::leveldb::crc32c::Value(data, raw.size() - ::leveldb::kBlockTrailerSize + 1);
        if (actual != crc) {
            return ::leveldb::Status::Corruption("block checksum mismatch");
        }
    }

    switch (data[raw.size() - ::leveldb::kBlockTrailerSize]) {
    case ::leveldb::kNoCompression:
        contents->data = ::leveldb::Slice(data, raw.size() - ::leveldb::kBlockTrailerSize);
        contents->heap_allocated = false;
        contents->cachable = false;
        break;
    case ::leveldb::kSnappyCompression: {
        size_t ulength = 0;
        if (::leveldb::port::Snappy_GetUncompressedLength(data, raw.size() - ::leveldb::kBlockTrailerSize, &ulength)) {
            return ::leveldb::Status::Corruption("corrupted block compressed");
        }
        char* ubuf = new char[ulength];
        if (::leveldb::port::Snappy_Uncompress(data, raw.size() - ::leveldb::kBlockTrailerSize, ubuf)) {
            delete ubuf;
            return ::leveldb::Status::Corruption("corrupted block compressed");
        }

        contents->data = ::leveldb::Slice(ubuf, ulength);
        contents->heap_allocated = true;
        contents->cachable = false;
        break;
    }
    default:
        return ::leveldb::Status::Corruption("bad block type");
    }

    return ::leveldb::Status::OK();
}

IndexBlock::IndexBlock(::leveldb::BlockContents& contents, ::leveldb::Options options)
    : options_(options) {
    block_ = new ::leveldb::Block(contents);
}

IndexBlock::~IndexBlock() {
    delete block_;
}

// convert internal leveldb key into user key iter
::leveldb::Iterator* IndexBlock::NewIterator() {
    return new KeyConvertingIterator(block_->NewIterator(options_.comparator));
}

}  // namespace mdt

