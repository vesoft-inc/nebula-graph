/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Base.h"
#include "common/time/Duration.h"
#include "common/encryption/MD5Utils.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/interface/gen-cpp2/common_constants.h"
#include "service/GraphService.h"
#include "service/RequestContext.h"
#include "service/GraphFlags.h"
#include "service/PasswordAuthenticator.h"
#include "service/CloudAuthenticator.h"
#include "stats/StatsDef.h"

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    sessionManager_ = std::make_unique<SessionManager>();
    queryEngine_ = std::make_unique<QueryEngine>();

    return queryEngine_->init(std::move(ioExecutor));
}


folly::Future<AuthResponse> GraphService::future_authenticate(
        const std::string& username,
        const std::string& password) {
    auto *peer = getRequestContext()->getPeerAddress();
    LOG(INFO) << "Authenticating user " << username << " from " <<  peer->describe();

    RequestContext<AuthResponse> ctx;
    auto session = sessionManager_->createSession();
    session->setAccount(username);
    ctx.setSession(std::move(session));

    if (!FLAGS_enable_authorize) {
        onHandle(ctx, nebula::cpp2::ErrorCode::SUCCEEDED);
    } else {
        auto code = auth(username, password);
        if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
            auto roles = queryEngine_->metaClient()->getRolesByUserFromCache(username);
            for (const auto& role : roles) {
                ctx.session()->setRole(role.get_space_id(), role.get_role_type());
            }
            onHandle(ctx, nebula::cpp2::ErrorCode::SUCCEEDED);
        } else {
            onHandle(ctx, code);
        }
    }

    ctx.finish();
    return ctx.future();
}


void GraphService::signout(int64_t sessionId) {
    VLOG(2) << "Sign out session " << sessionId;
    sessionManager_->removeSession(sessionId);
}


folly::Future<ExecutionResponse>
GraphService::future_execute(int64_t sessionId, const std::string& query) {
    auto ctx = std::make_unique<RequestContext<ExecutionResponse>>();
    ctx->setQuery(query);
    ctx->setRunner(getThreadManager());
    auto future = ctx->future();
    stats::StatsManager::addValue(kNumQueries);

    {
        // When the sessionId is 0, it means the clients to ping the connection is ok
        if (sessionId == 0) {
            ctx->resp().errorCode = ErrorCode::E_INVALID_SESSION;
            ctx->resp().errorMsg = std::make_unique<std::string>("Invalid session id");
            ctx->finish();
            return future;
        }
        auto result = sessionManager_->findSession(sessionId);
        if (!result.ok()) {
            FLOG_ERROR("Session not found, id[%ld]", sessionId);
            ctx->resp().errorCode = ErrorCode::E_SESSION_NOT_EXIST;
            ctx->resp().errorMsg = std::make_unique<std::string>(result.status().toString());
            ctx->finish();
            return future;
        }
        ctx->setSession(std::move(result).value());
    }
    queryEngine_->execute(std::move(ctx));

    return future;
}

const char* GraphService::getErrorStr(nebula::cpp2::ErrorCode result) {
    auto &errorMsgMap = nebula::cpp2::common_constants::ErrorMsgUTF8Map();
    auto findIter = errorMsgMap.find(result);
    if (findIter == errorMsgMap.end()) {
        auto error = folly::stringPrintf("Unknown error: %s(%d).",
                                          apache::thrift::util::enumNameSafe(result).c_str(),
                                          static_cast<int32_t>(result));
        LOG(ERROR) << error;
        return error.c_str();
    }

    auto resultIter = findIter->second.find(nebula::cpp2::Language::L_EN);
    if (resultIter != findIter->second.end()) {
        return resultIter->second.c_str();
    }
    return "Internal error: Unknown language L_EN";
}

void GraphService::onHandle(RequestContext<AuthResponse>& ctx, nebula::cpp2::ErrorCode code) {
    ctx.resp().errorCode = static_cast<nebula::ErrorCode>(code);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        sessionManager_->removeSession(ctx.session()->id());
        ctx.resp().errorMsg.reset(new std::string(getErrorStr(code)));
    } else {
        ctx.resp().sessionId.reset(new int64_t(ctx.session()->id()));
    }
}

nebula::cpp2::ErrorCode GraphService::auth(const std::string& username,
                                           const std::string& password) {
    if (FLAGS_auth_type == "password") {
        auto authenticator = std::make_unique<PasswordAuthenticator>(queryEngine_->metaClient());
        return authenticator->auth(username, encryption::MD5Utils::md5Encode(password));
    } else if (FLAGS_auth_type == "cloud") {
        auto authenticator = std::make_unique<CloudAuthenticator>(queryEngine_->metaClient());
        return authenticator->auth(username, password);
    }
    LOG(WARNING) << "Unknown auth type: " << FLAGS_auth_type;
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace graph
}  // namespace nebula
