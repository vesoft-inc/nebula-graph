/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "session/ClientSession.h"

#include "common/time/WallClock.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

ClientSession::ClientSession(meta::cpp2::Session &&session, meta::MetaClient* metaClient) {
    session_ = std::move(session);
    metaClient_ = metaClient;
}

std::shared_ptr<ClientSession> ClientSession::create(meta::cpp2::Session &&session,
                                                     meta::MetaClient* metaClient) {
    return std::shared_ptr<ClientSession>(new ClientSession(std::move(session), metaClient));
}

void ClientSession::charge() {
    folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
    idleDuration_.reset();
    session_.set_update_time(time::WallClock::fastNowInMicroSec());
}

uint64_t ClientSession::idleSeconds() {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return idleDuration_.elapsedInSec();
}

void ClientSession::addQuery(QueryContext* qctx) {
    auto epId = qctx->plan()->id();
    meta::cpp2::QueryDesc queryDesc;
    // queryDesc.set_start_time();
    queryDesc.set_status(meta::cpp2::QueryStatus::RUNNING);
    queryDesc.set_query(qctx->rctx()->query());

    folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
    contexts_.emplace(epId, qctx);
    session_.queries_ref()->emplace(epId, std::move(queryDesc));
}

void ClientSession::deleteQuery(QueryContext* qctx) {
    auto epId = qctx->plan()->id();
    folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
    contexts_.erase(epId);
    session_.queries_ref()->erase(epId);
}

}  // namespace graph
}  // namespace nebula
