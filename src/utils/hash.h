// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#ifndef MDT_UTIL_HASH_H_
#define MDT_UTIL_HASH_H_

#include <stddef.h>
#include <stdint.h>

namespace mdt {

extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

}

#endif  // STORAGE_LEVELDB_UTIL_HASH_H_
