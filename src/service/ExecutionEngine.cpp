/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "service/ExecutionEngine.h"

DECLARE_string(meta_server_addrs);

namespace nebula {
namespace graph {

ExecutionEngine::ExecutionEngine() {
}


ExecutionEngine::~ExecutionEngine() {
}


Status ExecutionEngine::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!addrs.ok()) {
        return addrs.status();
    }

    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                     std::move(addrs.value()),
                                                     HostAddr(0, 0),
                                                     0,
                                                     false);
    // load data try 3 time
    bool loadDataOk = metaClient_->waitForMetadReady(3);
    if (!loadDataOk) {
        // Resort to retrying in the background
        LOG(WARNING) << "Failed to synchronously wait for meta service ready";
    }

    schemaManager_ = meta::SchemaManager::create();
    schemaManager_->init(metaClient_.get());

    // gflagsManager_ = std::make_unique<meta::ClientBasedGflagsManager>(metaClient_.get());

    storage_ = std::make_unique<storage::GraphStorageClient>(ioExecutor,
                                                        metaClient_.get());
    return Status::OK();
}

void ExecutionEngine::execute(RequestContextPtr rctx) {
    UNUSED(rctx);
    // TODO:
    // 1. need context
    // 2. parse
    // 3. validate
    // 4. optional optimize
    // 5. execute
    // 6. response & release
}

}   // namespace graph
}   // namespace nebula
