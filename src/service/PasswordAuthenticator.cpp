/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/PasswordAuthenticator.h"

namespace nebula {
namespace graph {

PasswordAuthenticator::PasswordAuthenticator(const meta::MetaClient* client) {
    metaClient_ = client;
}

GraphStatus PasswordAuthenticator::auth(const std::string& user, const std::string& password) {
    auto code = metaClient_->authCheckFromCache(user, password);
    if (code == nebula::cpp2::ErrorCode::E_USERNAME_NOT_FOUND) {
        return GraphStatus::setUsernameNotFound();
    }
    if (code == nebula::cpp2::ErrorCode::E_INVALID_PASSWORD) {
        return GraphStatus::setInvalidPassword();
    }
    return GraphStatus::OK();
}

}   // namespace graph
}   // namespace nebula
