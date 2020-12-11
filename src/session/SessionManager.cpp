/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "session/SessionManager.h"
#include "service/GraphFlags.h"

namespace nebula {
namespace graph {

SessionManager::SessionManager(meta::MetaClient* metaClient, const HostAddr &hostAddr) {
    metaClient_ = metaClient;
    myAddr_ = hostAddr;
    scavenger_ = std::make_unique<thread::GenericWorker>();
    auto ok = scavenger_->start("session-manager");
    DCHECK(ok);
    scavenger_->addDelayTask(FLAGS_session_reclaim_interval_secs * 1000,
                             &SessionManager::threadFunc,
                             this);
}


SessionManager::~SessionManager() {
    if (scavenger_ != nullptr) {
        scavenger_->stop();
        scavenger_->wait();
        scavenger_.reset();
    }
}


std::shared_ptr<ClientSession> SessionManager::findSessionFromCache(SessionID id) {
    folly::RWSpinLock::ReadHolder rHolder(rwlock_);
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return nullptr;
    }
    VLOG(2) << "Find session from cache: " << id;
    return iter->second;
}


void SessionManager::findSessionFromMetad(
        SessionID id,
        folly::Executor* runner,
        std::unique_ptr<RequestContext<ExecutionResponse>> rctx,
        std::function<void(std::unique_ptr<RequestContext<ExecutionResponse>>)> execFunc) {
    VLOG(1) << "Find session `" << id << "' from metad";
    // local cache not found, need to get from metad
    auto addSession = [this, id] (auto &&resp) -> StatusOr<std::shared_ptr<ClientSession>> {
        if (!resp.ok()) {
            LOG(ERROR) << "Get session `" << id << "' failed:" <<  resp.status();
            return Status::Error(
                    "Session `%ld' not found: %s", id, resp.status().toString().c_str());
        }
        auto session = resp.value().get_session();
        auto spaceName = session.space_name;
        SpaceInfo spaceInfo;
        if (!spaceName.empty()) {
            VLOG(2) << "Get session with spaceName: " << spaceName;
            // get spaceInfo
            auto spaceId = metaClient_->getSpaceIdByNameFromCache(spaceName);
            if (!spaceId.ok()) {
                LOG(ERROR) << "Get session with Unknown space: " << spaceName;
            } else {
                auto spaceDesc = metaClient_->getSpaceDesc(spaceId.value());
                if (!spaceDesc.ok()) {
                    LOG(ERROR) << "Get session with Unknown space: " << spaceName;
                }
                spaceInfo.id = spaceId.value();
                spaceInfo.name = spaceName;
                spaceInfo.spaceDesc = std::move(spaceDesc).value();
            }
        }

        folly::RWSpinLock::WriteHolder wHolder(rwlock_);
        auto findPtr = activeSessions_.find(id);
        if (findPtr == activeSessions_.end()) {
            VLOG(1) << "Add session id: " << id << " from metad";
            auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
            sessionPtr->updateStatus(ClientSession::ClientStatus::kOnline);
            sessionPtr->charge();
            activeSessions_[id] = sessionPtr;

            // update the space info to sessionPtr
            if (!spaceName.empty()) {
                sessionPtr->setSpace(std::move(spaceInfo));
            }

            return sessionPtr;
        }
        return findPtr->second;
    };


    auto execInstance = new ExecuteInstance(std::move(rctx));
    auto doFinish = [this, execInstance, execFunc](auto&& resp) {
        VLOG(2) << "Find session doFinish";
        if (!resp.ok()) {
            execInstance->rctx()->resp().errorCode = ErrorCode::E_SESSION_INVALID;
            execInstance->rctx()->resp().errorMsg.reset(new std::string("Session not found."));
            execInstance->rctx()->finish();
            return execInstance->finish();
        }
        auto session = std::move(resp).value();
        session->updateGraphAddr(myAddr_);
        execInstance->rctx()->setSession(std::move(session));
        execFunc(execInstance->moveRctx());
        return execInstance->finish();
    };

    metaClient_->getSession(id).via(runner).then(addSession).then(doFinish);
}

void SessionManager::createSession(const std::string &userName,
                                   const std::string& clientIp,
                                   folly::Executor* runner,
                                   std::unique_ptr<RequestContext<AuthResponse>> rctx) {
    {
        folly::RWSpinLock::ReadHolder rHolder(rwlock_);
        if (activeSessions_.size() >= FLAGS_max_allowed_connections) {
            return rctx->finish();
        }
    }

    auto createCB = [this, userName = userName]
        (auto && resp) -> StatusOr<std::shared_ptr<ClientSession>> {
        if (!resp.ok()) {
            LOG(ERROR) << "Create session failed:" <<  resp.status();
            return Status::Error("Create session failed: %s", resp.status().toString().c_str());
        }
        auto session = resp.value().get_session();
        auto sid = session.session_id;
        DCHECK_NE(sid, 0L);
        {
            folly::RWSpinLock::WriteHolder wHolder(rwlock_);
            auto findPtr = activeSessions_.find(sid);
            if (findPtr == activeSessions_.end()) {
                VLOG(1) << "Create session id: " << sid << ", for user: " << userName;
                auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
                sessionPtr->updateStatus(ClientSession::ClientStatus::kOnline);
                sessionPtr->charge();
                activeSessions_[sid] = sessionPtr;
                return sessionPtr;
            }
            return findPtr->second;
        }
    };

    auto authInstance = new AuthInstance(std::move(rctx));
    auto doFinish = [this,
                     authInstance,
                     userName = userName,
                     clientIp = clientIp]
                             (folly::Try<StatusOr<std::shared_ptr<ClientSession>>>&& t) {
        VLOG(2) << "Create session doFinish";
        if (t.hasException()) {
            LOG(ERROR) << "Create session failed: " << t.exception().what();
            authInstance->rctx()->resp().errorCode = ErrorCode::E_RPC_FAILURE;
            std::string error = folly::stringPrintf("Create session failed: %s",
                    t.exception().what().c_str());
            authInstance->rctx()->resp().errorMsg.reset(new std::string(error));
            authInstance->rctx()->finish();
            return authInstance->finish();
        }
        auto ret = std::move(t).value();
        if (!ret.ok()) {
            LOG(ERROR) << "Create session for userName: " << userName
                       << ", ip: " << clientIp << " failed: " << ret.status();
            authInstance->rctx()->resp().errorCode = ErrorCode::E_SESSION_INVALID;
            authInstance->rctx()->resp().errorMsg.reset(new std::string("Create session failed."));
            authInstance->rctx()->finish();
            return authInstance->finish();
        }
        auto session = std::move(ret).value();
        auto sid = session->id();
        authInstance->rctx()->setSession(std::move(session));
        auto roles = metaClient_->getRolesByUserFromCache(userName);
        for (const auto &role : roles) {
            authInstance->rctx()->session()->setRole(role.get_space_id(), role.get_role_type());
        }
        authInstance->rctx()->resp().sessionId.reset(new SessionID(sid));
        authInstance->rctx()->finish();
        return authInstance->finish();
    };

    metaClient_->createSession(userName, myAddr_, clientIp)
        .via(runner)
        .then(createCB)
        .then(doFinish);
}


void SessionManager::removeSession(SessionID id) {
    folly::RWSpinLock::WriteHolder wHolder(rwlock_);
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return;
    }

    iter->second->updateStatus(ClientSession::ClientStatus::kRemove);
    auto resp = metaClient_->removeSession(id).get();
    if (!resp.ok()) {
        // it will delete by reclaim
        LOG(ERROR) << "Remove session `" << id << "' failed: " << resp.status();
    }
    activeSessions_.erase(iter);
}

void SessionManager::threadFunc() {
    reclaimExpiredSessions();
    UpdateSessionsToMeta();
    scavenger_->addDelayTask(FLAGS_session_reclaim_interval_secs * 1000,
                             &SessionManager::threadFunc,
                             this);
}

// TODO(dutor) Now we do a brute-force scanning, of course we could make it more efficient.
void SessionManager::reclaimExpiredSessions() {
    if (FLAGS_session_idle_timeout_secs == 0) {
        return;
    }

    folly::RWSpinLock::WriteHolder wHolder(rwlock_);
    if (activeSessions_.empty()) {
        return;
    }

    FVLOG3("Try to reclaim expired sessions out of %lu ones", activeSessions_.size());
    auto iter = activeSessions_.begin();
    auto end = activeSessions_.end();
    while (iter != end) {
        int32_t idleSecs = iter->second->idleSeconds();
        VLOG(2) << "SessionId: " << iter->first << ", idleSecs: " << idleSecs;
        if (idleSecs < FLAGS_session_idle_timeout_secs) {
            ++iter;
            continue;
        }
        FLOG_INFO("ClientSession %ld has expired", iter->first);

        iter->second->updateStatus(ClientSession::ClientStatus::kRemove);
        auto resp = metaClient_->removeSession(iter->first).get();
        if (!resp.ok()) {
            // TODO: Handle cases where the delete client failed
            LOG(ERROR) << "Remove session `" << iter->first << "' failed: " << resp.status();
        }
        iter = activeSessions_.erase(iter);
        // TODO: Disconnect the connection of the session
    }
}

void SessionManager::UpdateSessionsToMeta() {
    std::vector<meta::cpp2::Session> sessions;
    {
        folly::RWSpinLock::ReadHolder rHolder(rwlock_);
        if (activeSessions_.empty()) {
            return;
        }

        for (auto &ses : activeSessions_) {
            if (ses.second->getSession().graph_addr == myAddr_) {
                VLOG(3) << "Add Update session id: " << ses.second->getSession().session_id;
                sessions.emplace_back(ses.second->getSession());
            }
        }
    }
    auto resp = metaClient_->updateSessions(sessions).get();
    if (!resp.ok()) {
        LOG(ERROR) << "Update sessions failed: " << resp.status();
    }
}

}   // namespace graph
}   // namespace nebula
