/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/time/Duration.h"
#include "common/encryption/MD5Utils.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "service/GraphService.h"
#include "service/RequestContext.h"
#include "service/GraphFlags.h"
#include "service/PasswordAuthenticator.h"
#include "service/CloudAuthenticator.h"
#include "util/GraphStatus.h"

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    sessionManager_ = std::make_unique<SessionManager>();
    queryEngine_ = std::make_unique<QueryEngine>();

    return queryEngine_->init(std::move(ioExecutor));
}


folly::Future<cpp2::AuthResponse> GraphService::future_authenticate(
        const std::string& username,
        const std::string& password) {
    auto *peer = getConnectionContext()->getPeerAddress();
    LOG(INFO) << "Authenticating user " << username << " from " <<  peer->describe();

    RequestContext<cpp2::AuthResponse> ctx;
    auto session = sessionManager_->createSession();
    session->setAccount(username);
    ctx.setSession(std::move(session));

    if (!FLAGS_enable_authorize) {
        onHandle(ctx, GraphStatus::OK());
    } else {
        auto gStatus = auth(username, password);
        if (!gStatus.ok()) {
            auto roles = queryEngine_->metaClient()->getRolesByUserFromCache(username);
            for (const auto &role : roles) {
                ctx.session()->setRole(role.get_space_id(), role.get_role_type());
            }
        }
        onHandle(ctx, gStatus);
    }

    ctx.finish();
    return ctx.future();
}


void GraphService::signout(int64_t sessionId) {
    VLOG(2) << "Sign out session " << sessionId;
    sessionManager_->removeSession(sessionId);
}


folly::Future<cpp2::ExecutionResponse>
GraphService::future_execute(int64_t sessionId, const std::string& query) {
    auto ctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
    ctx->setQuery(query);
    ctx->setRunner(getThreadManager());
    auto future = ctx->future();
    {
        auto result = sessionManager_->findSession(sessionId);
        if (!result.ok()) {
            FLOG_ERROR("Session not found, id[%ld]", sessionId);
            ctx->resp().set_error_code(nebula::cpp2::ErrorCode::E_SESSION_NOT_EXIST);
            ctx->resp().set_error_msg(
                    folly::stringPrintf(
                        GraphStatus::getErrorMsg(
                            nebula::cpp2::ErrorCode::E_SESSION_NOT_EXIST).c_str(), sessionId));
            ctx->finish();
            return future;
        }
        ctx->setSession(std::move(result).value());
    }
    queryEngine_->execute(std::move(ctx));

    return future;
}

void GraphService::onHandle(RequestContext<cpp2::AuthResponse>& ctx,
                            GraphStatus gStatus) {
    ctx.resp().set_error_code(gStatus.getErrorCode());
    if (!gStatus.ok()) {
        sessionManager_->removeSession(ctx.session()->id());
        ctx.resp().set_error_msg(gStatus.toString());
    } else {
        ctx.resp().set_session_id(ctx.session()->id());
    }
}

GraphStatus GraphService::auth(const std::string& username, const std::string& password) {
    std::string authType = FLAGS_auth_type;
    folly::toLowerAscii(authType);
    if (!authType.compare("password")) {
        auto authenticator = std::make_unique<PasswordAuthenticator>(queryEngine_->metaClient());
        return authenticator->auth(username, encryption::MD5Utils::md5Encode(password));
    } else if (!authType.compare("cloud")) {
        auto authenticator = std::make_unique<CloudAuthenticator>(queryEngine_->metaClient());
        return authenticator->auth(username, password);
    }
    LOG(WARNING) << "Unknown auth type: " << authType;
    return GraphStatus::setInvalidAuthType(authType);
}

}  // namespace graph
}  // namespace nebula
