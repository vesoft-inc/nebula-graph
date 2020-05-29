/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

ExecutionContext::~ExecutionContext() {
    if (nullptr != sm_) {
        sm_ = nullptr;
    }
/*
    if (nullptr != gflagsManager_) {
        gflagsManager_ = nullptr;
    }
*/
    if (nullptr != storageClient_) {
        storageClient_ = nullptr;
    }

    if (nullptr != metaClient_) {
        metaClient_ = nullptr;
    }

    if (nullptr != charsetInfo_) {
        charsetInfo_ = nullptr;
    }
}

}   // namespace graph
}   // namespace nebula
