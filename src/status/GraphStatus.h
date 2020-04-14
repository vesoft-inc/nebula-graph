/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_GRAPHSTATUS_H
#define NEBULA_GRAPHSTATUS_H

#include "base/Base.h"
#include "base/Status.h"
#include "gen-cpp2/common_types.h"
#include "gen-cpp2/graph_types.h"
#include "gen-cpp2/meta_types.h"
#include "gen-cpp2/storage_types.h"
#include "gen-cpp2/error_msg_constants.h"

namespace nebula {
namespace graph {

enum class TypeName : uint8_t {
    UNKNOWN = 0,
    T_SPACE = 1,
    T_TAG = 2,
    T_EDGE = 3,
    T_INDEX = 4,
};

class GraphStatus final {
public:
    GraphStatus() = default;

    explicit GraphStatus(const Status &status) {
        UNUSED(status);
        setStatus(status);
    }

    explicit GraphStatus(const cpp2::ErrorCode code, std::string errorMsg) {
        errorCode_ = code;
        errorMsg_ = std::move(errorMsg);
    }

    ~GraphStatus() = default;

    // Init the language and the encoding
    static void init();

    static GraphStatus OK() {
        return GraphStatus();
    }

    template<typename RESP>
    static GraphStatus setMetaResponse(const RESP& resp,
                                       const std::string &name,
                                       const TypeName type);

    template<typename RESP>
    static GraphStatus setStorageResponse(const RESP& resp, const std::string &name);

    static std::string getErrorMsg(const cpp2::ErrorCode errorCode) {
        auto &errorMsgMap = cpp2::error_msg_constants::ErrorMsgUTF8Map();
        auto findIter = errorMsgMap.find(errorCode);
        if (findIter == errorMsgMap.end()) {
            return folly::stringPrintf("Unknown errorCode: %d", static_cast<int32_t>(errorCode));
        }

        auto resultIter = findIter->second.find(language);
        if (resultIter != findIter->second.end()) {
            return encode(resultIter->second);
        }
        return folly::stringPrintf("Unknown language: %d", static_cast<int32_t>(language));
    }

    static GraphStatus setStatus(const Status &status) {
        if (!status.ok()) {
            return GraphStatus(cpp2::ErrorCode::E_INTERNAL_ERROR,
                    folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_INTERNAL_ERROR).c_str(),
                        status.toString().c_str()));
        }
        return GraphStatus();
    }

    static GraphStatus setNotUseSpace() {
        return GraphStatus(cpp2::ErrorCode::E_NOT_USE_SPACE,
                getErrorMsg(cpp2::ErrorCode::E_NOT_USE_SPACE));
    }

    static GraphStatus setNoHosts() {
        return GraphStatus(cpp2::ErrorCode::E_NO_HOSTS,
                getErrorMsg(cpp2::ErrorCode::E_NO_HOSTS).c_str());
    }

    static GraphStatus setNoValidHost() {
        return GraphStatus(cpp2::ErrorCode::E_NO_VALID_HOST,
                getErrorMsg(cpp2::ErrorCode::E_NO_VALID_HOST).c_str());
    }

    static GraphStatus setMetaLeaderChange() {
        return GraphStatus(cpp2::ErrorCode::E_META_LEADER_CHANGE,
                getErrorMsg(cpp2::ErrorCode::E_META_LEADER_CHANGE).c_str());
    }

    static GraphStatus setSpaceNotFound(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_SPACE_NOT_FOUND,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_SPACE_NOT_FOUND).c_str(),
                    name.c_str()));
    }

    static GraphStatus setTagNotFound(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_TAG_NOT_FOUND,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_TAG_NOT_FOUND).c_str(),
                    name.c_str()));
    }

    static GraphStatus setEdgeNotFound(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_EDGE_NOT_FOUND,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_EDGE_NOT_FOUND).c_str(),
                    name.c_str()));
    }

    static GraphStatus setIndexNotFound(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_INDEX_NOT_FOUND,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_INDEX_NOT_FOUND).c_str(),
                    name.c_str()));
    }

    static GraphStatus setUserNotFound(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_USER_NOT_FOUND,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_USER_NOT_FOUND).c_str(),
                    name.c_str()));
    }

    static GraphStatus setConfigNotFound(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_CONFIG_NOT_FOUND,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_CONFIG_NOT_FOUND).c_str(),
                    name.c_str()));
    }

    static GraphStatus setColumnNotFound(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_COLUMN_NOT_FOUND,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_COLUMN_NOT_FOUND).c_str(),
                    name.c_str()));
    }

    static GraphStatus setSpaceExisted(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_SPACE_EXISTED,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_SPACE_EXISTED).c_str(),
                    name.c_str()));
    }

    static GraphStatus setTagExisted(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_TAG_EXISTED,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_TAG_EXISTED).c_str(),
                    name.c_str()));
    }

    static GraphStatus setEdgeExisted(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_EDGE_EXISTED,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_EDGE_EXISTED).c_str(),
                    name.c_str()));
    }

    static GraphStatus setIndexExisted(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_INDEX_EXISTED,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_INDEX_EXISTED).c_str(),
                    name.c_str()));
    }

    static GraphStatus setWrongColumnNumber() {
        return GraphStatus(cpp2::ErrorCode::E_WRONG_COLUMN_NUM,
                getErrorMsg(cpp2::ErrorCode::E_WRONG_COLUMN_NUM));
    }

    static GraphStatus setDefaultNotDefine(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_DEFAULT_NOT_DEFINE,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_DEFAULT_NOT_DEFINE).c_str(),
                    name.c_str()));
    }

    static GraphStatus setVariableNotDefined(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_VARIABLE_NOT_DEFINE,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_VARIABLE_NOT_DEFINE).c_str(),
                    name.c_str()));
    }

    static GraphStatus setSrcNotDefined(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_SRC_NOT_DEFINE,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_SRC_NOT_DEFINE).c_str(),
                    name.c_str()));
    }

    static GraphStatus setDstNotDefined(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_DST_NOT_DEFINE,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_DST_NOT_DEFINE).c_str(),
                    name.c_str()));
    }

    static GraphStatus setInvalidParam(const std::string &what) {
        return GraphStatus(cpp2::ErrorCode::E_INVALID_PARAM,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_INVALID_PARAM).c_str(),
                    what.c_str()));
    }

    static GraphStatus setConfigImmutable(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_CONFIG_IMMUTABLE,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_CONFIG_IMMUTABLE).c_str(),
                    name.c_str()));
    }

    static GraphStatus setCharsetNotSupport(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_CHARSET_NOT_SUPPORT,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_CHARSET_NOT_SUPPORT).c_str(),
                    name.c_str()));
    }

    static GraphStatus setCollationNotSupport(const std::string &name) {
        return GraphStatus(cpp2::ErrorCode::E_COLLACTION_NOT_SUPPORT,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_COLLACTION_NOT_SUPPORT).c_str(),
                    name.c_str()));
    }

    static GraphStatus setInternalError(const std::string &what) {
        return GraphStatus(cpp2::ErrorCode::E_INTERNAL_ERROR,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_INTERNAL_ERROR).c_str(),
                    what.c_str()));
    }

    static GraphStatus setNotComplete(const int32_t completeness) {
        return GraphStatus(cpp2::ErrorCode::E_NOT_COMPLETE,
                folly::stringPrintf(getErrorMsg(cpp2::ErrorCode::E_NOT_COMPLETE).c_str(),
                    completeness));
    }

    static GraphStatus setBalanced() {
        return GraphStatus(cpp2::ErrorCode::E_BALANCED,
                getErrorMsg(cpp2::ErrorCode::E_BALANCED).c_str());
    }

    static GraphStatus setBalancerRunning() {
        return GraphStatus(cpp2::ErrorCode::E_BALANCER_RUNNING,
                getErrorMsg(cpp2::ErrorCode::E_BALANCER_RUNNING).c_str());
    }

    static GraphStatus setBadBalancePlan() {
        return GraphStatus(cpp2::ErrorCode::E_BAD_BALANCE_PLAN,
                getErrorMsg(cpp2::ErrorCode::E_BAD_BALANCE_PLAN).c_str());
    }

    static GraphStatus setNoRunningBalancePlan() {
        return GraphStatus(cpp2::ErrorCode::E_NO_RUNNING_BALANCE_PLAN,
                getErrorMsg(cpp2::ErrorCode::E_NO_RUNNING_BALANCE_PLAN).c_str());
    }

    static GraphStatus setCorrupttedBlancePlan() {
        return GraphStatus(cpp2::ErrorCode::E_CORRUPTTED_BALANCE_PLAN,
                getErrorMsg(cpp2::ErrorCode::E_CORRUPTTED_BALANCE_PLAN).c_str());
    }

    static GraphStatus setLeaderChange() {
        return GraphStatus(cpp2::ErrorCode::E_LEADER_CHANGE,
                getErrorMsg(cpp2::ErrorCode::E_LEADER_CHANGE).c_str());
    }

    static GraphStatus setKeyHasExists() {
        return GraphStatus(cpp2::ErrorCode::E_KEY_HAS_EXISTS,
                getErrorMsg(cpp2::ErrorCode::E_KEY_HAS_EXISTS).c_str());
    }

    static GraphStatus setPartNotFound() {
        return GraphStatus(cpp2::ErrorCode::E_PART_NOT_FOUND,
                getErrorMsg(cpp2::ErrorCode::E_PART_NOT_FOUND).c_str());
    }

    static GraphStatus setKeyNotFound() {
        return GraphStatus(cpp2::ErrorCode::E_KEY_NOT_FOUND,
                getErrorMsg(cpp2::ErrorCode::E_KEY_NOT_FOUND).c_str());
    }

    static GraphStatus setConsensusError() {
        return GraphStatus(cpp2::ErrorCode::E_KEY_NOT_FOUND,
                getErrorMsg(cpp2::ErrorCode::E_KEY_NOT_FOUND).c_str());
    }

    static GraphStatus setImproperDataType() {
        return GraphStatus(cpp2::ErrorCode::E_IMPROPER_DATA_TYPE,
                getErrorMsg(cpp2::ErrorCode::E_IMPROPER_DATA_TYPE));
    }

    static GraphStatus setFailedToCheckpoint() {
        return GraphStatus(cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT,
                getErrorMsg(cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT).c_str());
    }

    static GraphStatus setCheckpointBlocked() {
        return GraphStatus(cpp2::ErrorCode::E_CHECK_POINT_BLOCKED,
                getErrorMsg(cpp2::ErrorCode::E_CHECK_POINT_BLOCKED).c_str());
    }

public:
    cpp2::ErrorCode getErrorCode() {
        return errorCode_;
    }

    std::string toString() {
        return errorMsg_;
    }

    bool ok() {
        return errorCode_ == cpp2::ErrorCode::SUCCEEDED;
    }

public:
    static cpp2::Language         language;
    static cpp2::Encoding         encoding;

private:
    static cpp2::Language toLanguage(const std::string &language);

    static cpp2::Encoding toEncoding(const std::string &encoding);

    static std::string encode(const std::string &errorMsg);

private:
    cpp2::ErrorCode                errorCode_{cpp2::ErrorCode::SUCCEEDED};
    std::string                    errorMsg_;
};
}   // namespace graph
}   // namespace nebula
#endif  // NEBULA_GRAPHSTATUS_H

