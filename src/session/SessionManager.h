/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SESSION_SESSIONMANAGER_H_
#define SESSION_SESSIONMANAGER_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "common/base/StatusOr.h"
#include "common/thread/GenericWorker.h"
#include "common/clients/meta/MetaClient.h"
#include "common/interface/gen-cpp2/GraphService.h"

#include "session/ClientSession.h"
#include "service/RequestContext.h"

/**
 * SessionManager manages the client sessions, e.g. create new, find existing and drop expired.
 */

namespace nebula {
namespace graph {

class SessionManager final {
public:
    SessionManager(meta::MetaClient* metaClient, const HostAddr &hostAddr);
    ~SessionManager();

    using SessionPtr = std::shared_ptr<ClientSession>;
    /**
     * Find an existing session
     */
    SessionPtr findSessionFromCache(SessionID id);

    using ExecFunc = std::function<void(std::unique_ptr<RequestContext<ExecutionResponse>>)>;
    void findSessionFromMetad(SessionID id,
                              folly::Executor* runner,
                              std::unique_ptr<RequestContext<ExecutionResponse>> rctx,
                              ExecFunc execFunc);
    /**
     * Create a new session
     */
    void createSession(const std::string &userName,
                       const std::string& clientIp,
                       folly::Executor* runner,
                       std::unique_ptr<RequestContext<AuthResponse>> rctx);
    /**
     * Remove a session
     */
    void removeSession(SessionID id);

private:
    void threadFunc();

    void reclaimExpiredSessions();

    void UpdateSessionsToMeta();


private:
    class AuthInstance {
    public:
        explicit AuthInstance(std::unique_ptr<RequestContext<AuthResponse>> rctx) {
            rctx_ = std::move(rctx);
        }

        ~AuthInstance() = default;

        RequestContext<AuthResponse>* rctx() const {
            return rctx_.get();
        }

        void finish() {
            delete this;
        }

    private:
        std::unique_ptr<RequestContext<AuthResponse>>               rctx_;
    };

    class ExecuteInstance {
    public:
        explicit ExecuteInstance(std::unique_ptr<RequestContext<ExecutionResponse>> rctx) {
            rctx_ = std::move(rctx);
        }

        ~ExecuteInstance() = default;

        RequestContext<ExecutionResponse>* rctx() const {
            return rctx_.get();
        }

        std::unique_ptr<RequestContext<ExecutionResponse>> moveRctx() {
            return std::move(rctx_);
        }

        void finish() {
            delete this;
        }

    private:
        std::unique_ptr<RequestContext<ExecutionResponse>>               rctx_;
    };

private:
    folly::RWSpinLock                           rwlock_;        // TODO(dutor) writer might starve
    std::unordered_map<SessionID, SessionPtr>   activeSessions_;
    std::unique_ptr<thread::GenericWorker>      scavenger_;
    meta::MetaClient                           *metaClient_{nullptr};
    HostAddr                                    myAddr_;
};

}   // namespace graph
}   // namespace nebula


#endif  // SESSION_SESSIONMANAGER_H_
