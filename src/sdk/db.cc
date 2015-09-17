// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/db_impl.h"

namespace mdt{

tatus Database::OpenDB(const std::string& db_name, Database** db_ptr) {
    return DatabaseImpl::OpenDB(db_name, db_ptr);
}

Database::~Database() {}

} // namespace mdt
