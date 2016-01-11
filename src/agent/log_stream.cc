#include "agent/log_stream.h"
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <pthread.h>
#include "agent/log_stream.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include "util/coding.h"

#include "leveldb/slice.h"
#include "leveldb/db.h"
#include "leveldb/status.h"
#include "proto/query.pb.h"

DECLARE_int32(file_stream_max_pending_request);

namespace mdt {
namespace agent {

void* LogStreamWrapper(void* arg) {
    LogStream* stream = (LogStream*)arg;
    stream->Run();
    return NULL;
}

LogStream::LogStream(std::string module_name, LogOptions log_options,
                     RpcClient* rpc_client, pthread_spinlock_t* server_addr_lock,
                     std::string* server_addr)
    : module_name_(module_name),
    log_options_(log_options),
    rpc_client_(rpc_client),
    server_addr_lock_(server_addr_lock),
    server_addr_(server_addr),
    stop_(false) {
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
    pthread_create(&tid_, NULL, LogStreamWrapper, this);
}

LogStream::~LogStream() {
    stop_ = true;
    thread_event_.Set();
    pthread_join(tid_, NULL);
    //pthread_spin_destroy(&lock_);
}

void LogStream::Run() {
    while (1) {
        thread_event_.Wait();
        if (stop_ == true) {
            break;
        }

        int64_t start_ts = mdt::timer::get_micros();

        std::set<std::string> local_write_event;
        std::set<std::string> local_delete_event;
        std::queue<DBKey*> local_key_queue;
        std::queue<DBKey*> local_failed_key_queue;

        pthread_spin_lock(&lock_);
        swap(local_write_event,write_event_);
        swap(local_delete_event, delete_event_);
        swap(key_queue_, local_key_queue);
        swap(failed_key_queue_, local_failed_key_queue);
        pthread_spin_unlock(&lock_);

        // handle push callback event
        while (!local_key_queue.empty()) {
            DBKey* key = local_key_queue.front();
            std::map<std::string, FileStream*>::iterator file_it = file_streams_.find(key->filename);
            FileStream* file_stream = NULL;
            if (file_it != file_streams_.end()) {
                file_stream = file_it->second;
            }
            local_key_queue.pop();
            // delete and free space
            if (file_stream) {
                file_stream->DeleteCheckoutPoint(key);
            }
            delete key;
        }

        // handle fail push callback
        while (!local_failed_key_queue.empty()) {
            DBKey* key = local_failed_key_queue.front();
            std::map<std::string, FileStream*>::iterator file_it = file_streams_.find(key->filename);
            FileStream* file_stream = NULL;
            if (file_it != file_streams_.end()) {
                file_stream = file_it->second;
            }

            local_failed_key_queue.pop();
            if (file_stream) {
                if (file_stream->HanleFailKey(key)) {
                    // re-send data
                    uint64_t offset, size;
                    file_stream->GetCheckpoint(key, &offset, &size);

                    if (size) {
                        std::vector<std::string> line_vec;
                        DBKey* rkey;
                        file_stream->CheckPointRead(&line_vec, &rkey, offset, size);

                        std::vector<mdt::SearchEngine::RpcStoreRequest*> req_vec;
                        ParseMdtRequest(line_vec, &req_vec);
                        AsyncPush(req_vec, rkey);
                    }
                }
            }
            delete key;
        }

        // handle write event
        std::set<std::string>::iterator write_it = local_write_event.begin();
        for(; write_it != local_write_event.end(); ++write_it) {
            const std::string& filename = *write_it;
            std::map<std::string, FileStream*>::iterator file_it = file_streams_.find(filename);
            FileStream* file_stream;
            if (file_it != file_streams_.end()) {
                file_stream = file_it->second;
            } else {
                int success;
                file_stream = new FileStream(module_name_, log_options_, filename, &success);
                if (success < 0) {
                    // TODO: do something
                    LOG(WARNING) << "new file stream " << filename << ", faile";
                }
                file_streams_[filename] = file_stream;
                // may take a little long
                ApplyRedoList(file_stream);
            }
            DBKey* key;
            std::vector<std::string> line_vec;
            if (file_stream->Read(&line_vec, &key) > 0) {
                std::vector<mdt::SearchEngine::RpcStoreRequest*> req_vec;
                ParseMdtRequest(line_vec, &req_vec);
                AsyncPush(req_vec, key);
            }
        }

        // handle delete event
        std::set<std::string>::iterator delete_it = local_delete_event.begin();
        for(; delete_it != local_delete_event.end(); ++delete_it) {
            const std::string& filename = *delete_it;
            std::map<std::string, FileStream*>::iterator file_it = file_streams_.find(filename);
            if (file_it != file_streams_.end()) {
                FileStream* file_stream = file_it->second;
                // if no one refer this file stream, then delete it
                if (file_stream->MarkDelete()) {
                    file_streams_.erase(file_it);
                    delete file_stream;
                }
            }
        }

        int64_t end_ts = mdt::timer::get_micros();
        VLOG(30) << "logstream run duration, " << end_ts - start_ts;
    }
}

// split string by substring
struct LogRecord {
    std::vector<std::string> columns;

    void Print() {
        std::cout << "LogRecord\n";
        for (int i = 0 ; i < (int)columns.size(); i++) {
            std::cout << "\t" << columns[i] << std::endl;
        }
    }

    int SplitLogItem(const std::string& str, const std::vector<std::string>& dim_vec) {
        std::size_t pos = 0, prev = 0;
        while (1) {
            std::size_t min_pos = std::string::npos;
            pos = min_pos;
            int min_idx = (int)(1 << 20);
            for (int i = 0; i < (int)dim_vec.size(); i++) {
                const std::string& dim = dim_vec[i];
                min_pos = str.find(dim, prev);
                if ((pos == std::string::npos) || (min_pos != std::string::npos && pos > min_pos)) {
                    pos = min_pos;
                    min_idx = i;
                }
            }
            if (pos > prev) {
                columns.push_back(str.substr(prev, pos - prev));
            }
            if ((pos == std::string::npos) || (min_idx == (int)(1 << 20))) {
                break;
            }
            prev = pos + dim_vec[min_idx].size();
        }
        return 0;
    }
};

// kv parser
// split string by char
struct LogTailerSpan {
    std::map<std::string, std::string> kv_annotation;

    uint32_t ParseKVpairs(const std::string& line, const std::string& linedelims,
                          const std::string& kvdelims,
                          const std::set<std::string>& index_list) {
        uint32_t size = line.size();
        if (size == 0) return 0;

        std::vector<std::string> linevec;
        boost::split(linevec, line, boost::is_any_of("\n"));
        if (linevec.size() == 0 || linevec[0].size() == 0) return 0;
        //if (linevec[0].at(linevec.size() - 1) != '\n') return 0;

        std::map<std::string, std::string>& logkv = kv_annotation;
        //logkv.clear();
        std::vector<std::string> kvpairs;
        boost::split(kvpairs, linevec[0], boost::is_any_of(linedelims));
        for (uint32_t i = 0; i < kvpairs.size(); i++) {
            const std::string& kvpair = kvpairs[i];
            std::vector<std::string> kv;
            boost::split(kv, kvpair, boost::is_any_of(kvdelims));
            if (kv.size() == 2 && kv[0].size() > 0 && kv[1].size() > 0) {
                if (index_list.find(kv[0]) == index_list.end()) {
                    logkv.insert(std::pair<std::string, std::string>(kv[0], kv[1]));
                }
            }
        }
        return linevec[0].size() + 1;
    }

    uint32_t ParseFixedKvPairs(const std::string& line, const std::string& linedelims,
                               const std::map<std::string, int>& fixed_index_list) {
        uint32_t size = line.size();
        if (size == 0) return 0;

        std::map<std::string, std::string>& logkv = kv_annotation;
        //logkv.clear();
        std::vector<std::string> kvpairs;
        boost::split(kvpairs, line, boost::is_any_of(linedelims));
        std::map<std::string, int>::const_iterator it = fixed_index_list.begin();
        for (; it != fixed_index_list.end(); ++it) {
            if ((uint32_t)it->second < kvpairs.size()) {
                logkv[it->first] = kvpairs[it->second];
            }
        }
        return 0;
    }

    void PrintKVpairs() {
        const std::map<std::string, std::string>& logkv = kv_annotation;
        std::cout << "LogSpan kv: ";
        std::map<std::string, std::string>::const_iterator it = logkv.begin();
        for (; it != logkv.end(); ++it) {
            std::cout << "[" << it->first << ":" << it->second << "]  ";
        }
        std::cout << std::endl;
    }
};

// type 1: sec + micro sec
uint64_t LogStream::ParseTime(const std::string& time_str) {
    if (time_type_ == 1) {
        return (uint64_t)atol(time_str.c_str());
    }
    return 0;
}

// NOTICE: parse log line, add monitor logic
// case 1: key1=001,key2=002,key3=003||key4=004,key005=005,key006=006
// case 2: 001 002 003 004 005 006
// case 3: key1 001, key2 002, key3 003, key4 004, key5 005, key6 006
int LogStream::ParseMdtRequest(std::vector<std::string>& line_vec,
                               std::vector<mdt::SearchEngine::RpcStoreRequest* >* req_vec) {
    for (uint32_t i = 0; i < line_vec.size(); i++) {
        int res = 0;
        std::string& line  = line_vec[0];
        mdt::SearchEngine::RpcStoreRequest* req = NULL;

        LogRecord log;
        if (log.SplitLogItem(line, string_delims_) < 0) {
            res = -1;
        } else {
            LogTailerSpan kv;
            for (int i = 0; i < (int)log.columns.size(); i++) {
                if (use_fixed_index_list_) {
                    kv.ParseFixedKvPairs(log.columns[i], line_delims_, fixed_index_list_);
                } else {
                    kv.ParseKVpairs(log.columns[i], line_delims_, kv_delims_, index_list_);
                }
            }

            req = new mdt::SearchEngine::RpcStoreRequest();
            req->set_db_name(db_name_);
            req->set_table_name(table_name_);
            // set index
            std::map<std::string, std::string>::iterator it = kv.kv_annotation.begin();
            for (; it != kv.kv_annotation.end(); ++it) {
                if (it->first == primary_key_) {
                    req->set_primary_key(it->second);
                } else {
                    mdt::SearchEngine::RpcStoreIndex* idx = req->add_index_list();
                    idx->set_index_table(it->first);
                    idx->set_key(it->second);
                }
            }
            // if primary key not set, use time
            if (req->primary_key() == "") {
                res = -1;
                //req->set_primary_key(mdt::timer::get_micros());
            }
            req->set_data(line);
            // user has time item in log
            it = kv.kv_annotation.find(user_time_name_);
            if (user_time_name_.size() && it != kv.kv_annotation.end()) {
                uint64_t ts = ParseTime(it->second);
                if (ts > 0) {
                    req->set_timestamp(ts);
                } else {
                    req->set_timestamp(mdt::timer::get_micros());
                }
            } else {
                req->set_timestamp(mdt::timer::get_micros());
            }
        }

        if (res < 0 && req) {
            delete req;
        } else {
            req_vec->push_back(req);
        }
    }
    return 0;
}

void LogStream::ApplyRedoList(FileStream* file_stream) {
    // redo check point
    std::map<uint64_t, uint64_t> redo_list;
    file_stream->GetRedoList(&redo_list);
    std::map<uint64_t, uint64_t>::iterator redo_it = redo_list.begin();
    for (; redo_it != redo_list.end(); ++redo_it) {
        DBKey* key;
        std::vector<std::string> line_vec;
        file_stream->CheckPointRead(&line_vec, &key, redo_it->first, redo_it->second);

        std::vector<mdt::SearchEngine::RpcStoreRequest*> req_vec;
        ParseMdtRequest(line_vec, &req_vec);
        AsyncPush(req_vec, key);
    }
}

int LogStream::AsyncPush(std::vector<mdt::SearchEngine::RpcStoreRequest*>& req_vec, DBKey* key) {
    mdt::SearchEngine::SearchEngineService_Stub* service;
    pthread_spin_lock(server_addr_lock_);
    std::string server_addr = *server_addr_;
    pthread_spin_unlock(server_addr_lock_);
    rpc_client_->GetMethodList(server_addr, &service);

    for (uint32_t i = 0; i < req_vec.size(); i++) {
        mdt::SearchEngine::RpcStoreRequest* req = req_vec[i];
        mdt::SearchEngine::RpcStoreResponse* resp = new mdt::SearchEngine::RpcStoreResponse;

        boost::function<void (const mdt::SearchEngine::RpcStoreRequest*,
                              mdt::SearchEngine::RpcStoreResponse*,
                              bool, int)> callback =
            boost::bind(&LogStream::AsyncPushCallback,
                        this, _1, _2, _3, _4, service, key);
        rpc_client_->AsyncCall(service,
                              &mdt::SearchEngine::SearchEngineService_Stub::Store,
                              req, resp, callback);
    }
    return 0;
}

void LogStream::AsyncPushCallback(const mdt::SearchEngine::RpcStoreRequest* req,
                                  mdt::SearchEngine::RpcStoreResponse* resp,
                                  bool failed, int error,
                                  mdt::SearchEngine::SearchEngineService_Stub* service,
                                  DBKey* key) {
    // handle data push error
    if (failed) {
        LOG(WARNING) << "async push error, " << error;
        pthread_spin_lock(&lock_);
        failed_key_queue_.push(key);
        pthread_spin_unlock(&lock_);
        thread_event_.Set();
    } else {
        pthread_spin_lock(&lock_);
        key_queue_.push(key);
        pthread_spin_unlock(&lock_);
        thread_event_.Set();
    }
    delete req;
    delete resp;
    delete service;
}

int LogStream::AddWriteEvent(std::string filename) {
    pthread_spin_lock(&lock_);
    write_event_.insert(filename);
    pthread_spin_unlock(&lock_);
    thread_event_.Set();
    return 0;
}

int LogStream::DeleteWatchEvent(std::string filename) {
    pthread_spin_lock(&lock_);
    delete_event_.insert(filename);
    pthread_spin_unlock(&lock_);
    thread_event_.Set();
    return 0;
}

//////////////////////////////////////////
//      FileStream implementation       //
//////////////////////////////////////////
FileStream::FileStream(std::string module_name, LogOptions log_options,
                       std::string filename, int* success)
    : module_name_(module_name),
    filename_(filename),
    log_options_(log_options) {
    *success = -1;
    fd_ = open(filename.c_str(), O_RDONLY);
    if (fd_ < 0) {
        return;
    }
    current_offset_ = 0;
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);

    // recovery restart point
    *success = RecoveryCheckPoint();
    if (*success < 0) {
        close(fd_);
    }
}

FileStream::~FileStream() {

}

void FileStream::GetRedoList(std::map<uint64_t, uint64_t>* redo_list) {
    pthread_spin_lock(&lock_);
    swap(*redo_list, redo_list_);
    pthread_spin_unlock(&lock_);
}

// big endian
void FileStream::EncodeUint64BigEndian(uint64_t value, std::string* str) {
    char offset_buf[8];
    EncodeBigEndian(offset_buf, value);
    std::string offset_str(offset_buf, 8);
    *str = offset_str;
}

void FileStream::MakeKeyValue(const std::string& module_name,
                               const std::string& filename,
                               uint64_t offset,
                               std::string* key,
                               uint64_t size,
                               std::string* value) {
    *key = module_name;
    key->push_back('\0');
    *key += filename;
    key->push_back('\0');

    std::string offset_str;
    EncodeUint64BigEndian(offset, &offset_str);
    *key += offset_str;

    if (value) {
        EncodeUint64BigEndian(size, value);
    }
    return;
}

void FileStream::ParseKeyValue(const leveldb::Slice& key,
                               const leveldb::Slice& value,
                               uint64_t* offset, uint64_t* size) {
    int mlen = strlen(key.data());
    //leveldb::Slice module_name = leveldb::Slice(key.data(), mlen);
    int flen = strlen(key.data() + mlen + 1);
    //leveldb::Slice filename = leveldb::Slice(key.data() + mlen + 1, flen);
    leveldb::Slice offset_str = leveldb::Slice(key.data() + mlen + 1 + flen + 1, 8);
    *offset = DecodeBigEndain(offset_str.data());
    *size = DecodeBigEndain(value.data());
}

// use leveldb recovery mem cp list
int FileStream::RecoveryCheckPoint() {
    int64_t begin_ts = timer::get_micros();
    int64_t end_ts;
    leveldb::Iterator* db_it = log_options_.db->NewIterator(leveldb::ReadOptions());

    std::string startkey, endkey;
    MakeKeyValue(module_name_, filename_, 0, &startkey, 0, NULL);
    MakeKeyValue(module_name_, filename_, 0xffffffffffffffff, &endkey, 0, NULL);
    for (db_it->Seek(startkey);
         db_it->Valid() && db_it->key().ToString() < endkey;
         db_it->Next()) {
        leveldb::Slice key = db_it->key();
        leveldb::Slice value = db_it->value();
        uint64_t offset, size;
        ParseKeyValue(key, value, &offset, &size);

        // insert [offset, size] into mem cp list
        pthread_spin_lock(&lock_);
        std::map<uint64_t, uint64_t>::iterator cp_it =  mem_checkpoint_list_.find(offset);
        if (cp_it != mem_checkpoint_list_.end()) {
            uint64_t tmp_size = cp_it->second;
            if (size > tmp_size) {
                cp_it->second = size;
                redo_list_[offset] = size;
            }
        } else {
            mem_checkpoint_list_[offset] = size;
            redo_list_[offset] = size;
        }
        pthread_spin_unlock(&lock_);

        // update current_offset
        if (current_offset_ < offset + size) {
            current_offset_ = offset + size;
        }
    }

    delete db_it;
    end_ts = timer::get_micros();
    LOG(INFO) << "recovery " << filename_ << ", cost time " << end_ts - begin_ts;
    return 0;
}

void FileStream::GetCheckpoint(DBKey* key, uint64_t* offset, uint64_t* size) {
    *size = 0;
    pthread_spin_lock(&lock_);
    std::map<uint64_t, uint64_t>::iterator it = mem_checkpoint_list_.find(key->offset);
    if (it != mem_checkpoint_list_.end()) {
        *offset = it->first;
        *size = it->second;
    }
    pthread_spin_unlock(&lock_);
}

ssize_t FileStream::ParseLine(char* buf, ssize_t size, std::vector<std::string>* line_vec) {
    if (size <= 0) {
        return -1;
    }
    ssize_t res = 0;
    std::string str(buf, size);
    boost::split(*line_vec, str, boost::is_any_of("\n"));
    if (buf[size -1] != '\n') {
        line_vec->pop_back();
    }
    for (uint32_t i = 0; i < line_vec->size(); i++) {
        res += line_vec[i].size() + 1;
    }
    return res;
}

// each read granularity is 64KB
int FileStream::Read(std::vector<std::string>* line_vec, DBKey** key) {
    int ret = 0;
    if (fd_ > 0) {
        // check mem cp pending request
        pthread_spin_lock(&lock_);
        if (mem_checkpoint_list_.size() > (uint32_t)FLAGS_file_stream_max_pending_request) {
            pthread_spin_unlock(&lock_);
            return -1;
        }
        pthread_spin_unlock(&lock_);

        uint64_t size = 65536;
        uint64_t offset = current_offset_;
        char* buf = new char[size];
        ssize_t res = pread(fd_, buf, size, offset);
        if (res < 0) {
            LOG(WARNING) << "redo cp, read file error " << offset << ", size " << size << ", res " << res;
            ret = -1;
        }
        res = ParseLine(buf, res, line_vec);
        delete buf;

        if (res <= 0) {
            ret = -1;
        } else {
            *key = new DBKey;
            (*key)->filename = filename_;
            (*key)->offset = offset;

            ret = LogCheckPoint(offset, res);
            if (ret < 0) {
                delete (*key);
                line_vec->clear();
            }
        }
    }
    return ret;
}

int FileStream::LogCheckPoint(uint64_t offset, uint64_t size) {
    int ret = 0;
    pthread_spin_lock(&lock_);
    mem_checkpoint_list_[offset] = size;
    pthread_spin_unlock(&lock_);

    std::string key, value;
    MakeKeyValue(module_name_, filename_, offset, &key, size, &value);
    leveldb::Status s = log_options_.db->Put(leveldb::WriteOptions(), key, value);
    if (!s.ok()) {
        pthread_spin_lock(&lock_);
        std::map<uint64_t, uint64_t>::iterator it = mem_checkpoint_list_.find(offset);
        if (it != mem_checkpoint_list_.end()) {
            mem_checkpoint_list_.erase(it);
        }
        pthread_spin_unlock(&lock_);
        ret = -1;
    } else {
        // write db success
        current_offset_ = offset + size;
    }
    return ret;
}

int FileStream::DeleteCheckoutPoint(DBKey* key) {
    // delete mem checkpoint
    pthread_spin_lock(&lock_);
    std::map<uint64_t, uint64_t>::iterator it =  mem_checkpoint_list_.find(key->offset);
    if (it != mem_checkpoint_list_.end()) {
        mem_checkpoint_list_.erase(it);
    }
    pthread_spin_unlock(&lock_);

    // delete log checkpoint
    std::string key_str;
    MakeKeyValue(module_name_, filename_, key->offset, &key_str, 0, NULL);
    leveldb::Status s = log_options_.db->Delete(leveldb::WriteOptions(), key_str);
    if (!s.ok()) {
        LOG(WARNING) << "delete db checkpoint error, " << filename_ << ", offset " << key->offset;
    }
    return 0;
}

int FileStream::CheckPointRead(std::vector<std::string>* line_vec, DBKey** key,
                               uint64_t offset, uint64_t size) {
    int ret = 0;
    if (fd_ > 0) {
        char* buf = new char[size];
        ssize_t res = pread(fd_, buf, size, offset);
        if (res < (int64_t)size) {
            LOG(WARNING) << "redo cp, read file error " << offset << ", size " << size << ", res " << res;
            ret = -1;
        }
        res = ParseLine(buf, res, line_vec);
        if (res != (int64_t)size) {
            LOG(WARNING) << "redo cp, parse buf, size not match, offset " << offset << ", size " << size << ", res " << res;
            ret = -1;
        } else {
            *key = new DBKey;
            (*key)->filename = filename_;
            (*key)->offset = offset;
        }
        delete buf;
    }
    return ret;
}

int FileStream::HanleFailKey(DBKey* key) {
    // TODO: mark fail, retry and change channel
    return 1;
}

int FileStream::MarkDelete() {
    close(fd_);
    return 1;
}

}
}

