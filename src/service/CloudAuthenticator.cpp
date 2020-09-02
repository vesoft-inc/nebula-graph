/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/json.h>

#include "common/encryption/Base64.h"
#include "common/http/HttpClient.h"
#include "service/CloudAuthenticator.h"
#include "service/GraphFlags.h"

namespace nebula {
namespace graph {

CloudAuthenticator::CloudAuthenticator(const meta::MetaClient* client) {
    metaClient_ = client;
}

GraphStatus CloudAuthenticator::auth(const std::string& user, const std::string& password) {
    // The shadow account on the nebula side has been created
    // Normal passwords and tokens use different prefixes

    // First, go to meta to check if the shadow account exists
    if (!metaClient_->checkShadowAccountFromCache(user)) {
        return GraphStatus::setUsernameNotFound();
    }

    // Second, use user + password authentication methods
    StatusOr<std::string> result;
    std::string userAndPasswd = user + ":" + password;
    std::string base64Str = encryption::Base64::encode(userAndPasswd);

    std::string header = "-H \"Content-Type: application/json\"  -H \"Authorization:Nebula ";
    header =  header + base64Str + "\"";
    result = http::HttpClient::post(FLAGS_cloud_http_url, header);

    if (!result.ok()) {
        LOG(ERROR) << result.status();
        return GraphStatus::setInvalidPassword();
    }

    try {
        auto json = folly::parseJson(result.value());
        if (json["code"].asString().compare("0") != 0) {
            LOG(ERROR) << "Cloud authentication failed, user: " << user;
            return GraphStatus::setInvalidPassword();
        }
    } catch (std::exception& e) {
        LOG(ERROR) << "Invalid json: " << e.what();
        GraphStatus::setInvalidPassword();
    }
    return GraphStatus::OK();
}

}   // namespace graph
}   // namespace nebula
