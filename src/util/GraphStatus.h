/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef NEBULA_GRAPHSTATUS_H
#define NEBULA_GRAPHSTATUS_H

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "common/interface/gen-cpp2/error_msg_constants.h"

namespace nebula {
namespace graph {

class GraphStatus final {
public:
    using ErrorCode = nebula::cpp2::ErrorCode;
    GraphStatus() = default;

    explicit GraphStatus(const ErrorCode code, std::string errorMsg) {
        errorCode_ = code;
        errorMsg_ = std::move(errorMsg);
    }

    ~GraphStatus() = default;

    // Init the language and the encoding
    static void init();

    static GraphStatus OK() {
        return GraphStatus();
    }

    static GraphStatus from(const GraphStatus &s) {
        return s;
    }

    static GraphStatus setRpcResponse(const ErrorCode code,
                                      const std::string &name);

    static std::string getErrorMsg(const ErrorCode errorCode) {
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

    static GraphStatus setRpcFailed(const std::string& reason) {
        return GraphStatus(ErrorCode::E_RPC_FAILED,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_RPC_FAILED).c_str(), reason.c_str()));
    }

    static GraphStatus setLeaderChanged() {
        return GraphStatus(ErrorCode::E_LEADER_CHANGED,
                           getErrorMsg(ErrorCode::E_LEADER_CHANGED));
    }

    static GraphStatus setBadUsernamePassword() {
        return GraphStatus(ErrorCode::E_BAD_USERNAME_PASSWORD,
                           getErrorMsg(ErrorCode::E_BAD_USERNAME_PASSWORD));
    }

    static GraphStatus setSessionInvalid(const int64_t sessionId) {
        return GraphStatus(ErrorCode::E_SESSION_INVALID,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_SESSION_INVALID).c_str(), sessionId));
    }

    static GraphStatus setSyntaxError(const std::string& string) {
        return GraphStatus(ErrorCode::E_SYNTAX_ERROR,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_SYNTAX_ERROR).c_str(), string.c_str()));
    }

    static GraphStatus setSemanticError(const std::string& string) {
        return GraphStatus(ErrorCode::E_SEMANTIC_ERROR,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_SEMANTIC_ERROR).c_str(), string.c_str()));
    }

    static GraphStatus setNotUseSpace() {
        return GraphStatus(ErrorCode::E_NOT_USE_SPACE,
                           getErrorMsg(ErrorCode::E_NOT_USE_SPACE));
    }

    static GraphStatus setInvalidRank() {
        return GraphStatus(ErrorCode::E_INVALID_RANK,
                           getErrorMsg(ErrorCode::E_INVALID_RANK));
    }

    static GraphStatus setInvalidEdgeType() {
        return GraphStatus(ErrorCode::E_INVALID_EDGE_TYPE,
                           getErrorMsg(ErrorCode::E_INVALID_EDGE_TYPE));
    }

    static GraphStatus setOutOfLenOfUsername() {
        return GraphStatus(ErrorCode::E_OUT_OF_LEN_OF_USERNAME,
                           getErrorMsg(ErrorCode::E_OUT_OF_LEN_OF_USERNAME));
    }

    static GraphStatus setOutOfLenOfPassword() {
        return GraphStatus(ErrorCode::E_OUT_OF_LEN_OF_PASSWORD,
                           getErrorMsg(ErrorCode::E_OUT_OF_LEN_OF_PASSWORD));
    }

    static GraphStatus setDuplicateColumnName(const std::string &name) {
        return GraphStatus(ErrorCode::E_DUPLICATE_COLUMN_NAME,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_DUPLICATE_COLUMN_NAME).c_str(), name.c_str()));
    }

    static GraphStatus setColumnCountNotMatch() {
        return GraphStatus(ErrorCode::E_COLUMN_COUNT_NOT_MATCH,
                           getErrorMsg(ErrorCode::E_COLUMN_COUNT_NOT_MATCH));
    }

    static GraphStatus setColumnNotFound(const std::string &name) {
        return GraphStatus(ErrorCode::E_COLUMN_NOT_FOUND,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_COLUMN_NOT_FOUND).c_str(), name.c_str()));
    }

    static GraphStatus setUnsupportedExpr(const std::string &expr) {
        return GraphStatus(ErrorCode::E_UNSUPPORTED_EXPR,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_UNSUPPORTED_EXPR).c_str(), expr.c_str()));
    }

    static GraphStatus setOutOfMaxStatements() {
        return GraphStatus(ErrorCode::E_OUT_OF_MAX_STATEMENTS,
                           getErrorMsg(ErrorCode::E_OUT_OF_MAX_STATEMENTS));
    }

    static GraphStatus setPermissionDenied() {
        return GraphStatus(ErrorCode::E_PERMISSION_DENIED,
                           getErrorMsg(ErrorCode::E_PERMISSION_DENIED));
    }

    static GraphStatus setNoTags() {
        return GraphStatus(ErrorCode::E_NO_TAGS,
                           getErrorMsg(ErrorCode::E_NO_TAGS));
    }

    static GraphStatus setNoEdges() {
        return GraphStatus(ErrorCode::E_NO_EDGES,
                           getErrorMsg(ErrorCode::E_NO_EDGES));
    }

    static GraphStatus setInvalidExpr(const std::string &exprStr) {
        return GraphStatus(ErrorCode::E_INVALID_EXPR,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_INVALID_EXPR).c_str(), exprStr.c_str()));
    }

    static GraphStatus setNoHosts() {
        return GraphStatus(ErrorCode::E_NO_HOSTS,
                           getErrorMsg(ErrorCode::E_NO_HOSTS));
    }

    static GraphStatus setSpaceExisted(const std::string &name) {
        return GraphStatus(ErrorCode::E_SPACE_EXISTED,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_SPACE_EXISTED).c_str(), name.c_str()));
    }

    static GraphStatus setTagExisted(const std::string &name) {
        return GraphStatus(ErrorCode::E_TAG_EXISTED,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_TAG_EXISTED).c_str(), name.c_str()));
    }

    static GraphStatus setEdgeExisted(const std::string &name) {
        return GraphStatus(ErrorCode::E_EDGE_EXISTED,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_EDGE_EXISTED).c_str(), name.c_str()));
    }

    static GraphStatus setIndexExisted(const std::string &name) {
        return GraphStatus(ErrorCode::E_INDEX_EXISTED,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_INDEX_EXISTED).c_str(), name.c_str()));
    }

    static GraphStatus setUnsupported() {
        return GraphStatus(ErrorCode::E_UNSUPPORTED,
                           getErrorMsg(ErrorCode::E_UNSUPPORTED));
    }

    static GraphStatus setNotDropProp() {
        return GraphStatus(ErrorCode::E_NOT_DROP_PROP,
                           getErrorMsg(ErrorCode::E_NOT_DROP_PROP));
    }

    static GraphStatus setConfigImmutable(const std::string &name) {
        return GraphStatus(ErrorCode::E_CONFIG_IMMUTABLE,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_CONFIG_IMMUTABLE).c_str(), name.c_str()));
    }

    static GraphStatus setConflict(const std::string &name) {
        return GraphStatus(ErrorCode::E_CONFLICT,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_CONFLICT).c_str(), name.c_str()));
    }

    static GraphStatus setInvalidParam(const std::string &name) {
        return GraphStatus(ErrorCode::E_INVALID_PARAM,
                   folly::stringPrintf(
                           getErrorMsg(ErrorCode::E_INVALID_PARAM).c_str(), name.c_str()));
    }

    static GraphStatus setStoreFailed() {
        return GraphStatus(ErrorCode::E_STORE_FAILED,
                           getErrorMsg(ErrorCode::E_STORE_FAILED));
    }

    static GraphStatus setStoreSegmentIllegal() {
        return GraphStatus(ErrorCode::E_STORE_SEGMENT_ILLEGAL,
                           getErrorMsg(ErrorCode::E_STORE_SEGMENT_ILLEGAL));
    }

    static GraphStatus setBalanced() {
        return GraphStatus(ErrorCode::E_BALANCED,
                           getErrorMsg(ErrorCode::E_BALANCED));
    }

    static GraphStatus setBalancerRunning() {
        return GraphStatus(ErrorCode::E_BALANCER_RUNNING,
                           getErrorMsg(ErrorCode::E_BALANCER_RUNNING));
    }

    static GraphStatus setBadBalancePlan() {
        return GraphStatus(ErrorCode::E_BAD_BALANCE_PLAN,
                           getErrorMsg(ErrorCode::E_BAD_BALANCE_PLAN));
    }

    static GraphStatus setNoRunningBalancePlan() {
        return GraphStatus(ErrorCode::E_NO_RUNNING_BALANCE_PLAN,
                           getErrorMsg(ErrorCode::E_NO_RUNNING_BALANCE_PLAN));
    }

    static GraphStatus setCorrupttedBalancePlan() {
        return GraphStatus(ErrorCode::E_CORRUPTTED_BALANCE_PLAN,
                           getErrorMsg(ErrorCode::E_CORRUPTTED_BALANCE_PLAN));
    }

    static GraphStatus setNoValidHost() {
        return GraphStatus(ErrorCode::E_NO_VALID_HOST,
                           getErrorMsg(ErrorCode::E_NO_VALID_HOST));
    }

    static GraphStatus setInvalidPassword() {
        return GraphStatus(ErrorCode::E_INVALID_PASSWORD,
                           getErrorMsg(ErrorCode::E_INVALID_PASSWORD));
    }

    static GraphStatus setImproperRole() {
        return GraphStatus(ErrorCode::E_IMPROPER_ROLE,
                           getErrorMsg(ErrorCode::E_IMPROPER_ROLE));
    }

    static GraphStatus setInvalidPartitionNum() {
        return GraphStatus(ErrorCode::E_INVALID_PARTITION_NUM,
                           getErrorMsg(ErrorCode::E_INVALID_PARTITION_NUM));
    }

    static GraphStatus setInvalidReplicaFactor() {
        return GraphStatus(ErrorCode::E_INVALID_REPLICA_FACTOR,
                           getErrorMsg(ErrorCode::E_INVALID_REPLICA_FACTOR));
    }

    static GraphStatus setInvalidCharset() {
        return GraphStatus(ErrorCode::E_INVALID_CHARSET,
                           getErrorMsg(ErrorCode::E_INVALID_CHARSET));
    }

    static GraphStatus setInvalidCollate() {
        return GraphStatus(ErrorCode::E_INVALID_COLLATE,
                           getErrorMsg(ErrorCode::E_INVALID_COLLATE));
    }

    static GraphStatus setCharsetCollateNotMatch() {
        return GraphStatus(ErrorCode::E_CHARSET_COLLATE_NOT_MATCH,
                           getErrorMsg(ErrorCode::E_CHARSET_COLLATE_NOT_MATCH));
    }

    static GraphStatus setSnapshotFailed() {
        return GraphStatus(ErrorCode::E_SNAPSHOT_FAILED,
                           getErrorMsg(ErrorCode::E_SNAPSHOT_FAILED));
    }

    static GraphStatus setBlockWriteFailed() {
        return GraphStatus(ErrorCode::E_BLOCK_WRITE_FAILED,
                           getErrorMsg(ErrorCode::E_BLOCK_WRITE_FAILED));
    }

    static GraphStatus setRebuildIndexFailed() {
        return GraphStatus(ErrorCode::E_REBUILD_INDEX_FAILED,
                           getErrorMsg(ErrorCode::E_REBUILD_INDEX_FAILED));
    }

    static GraphStatus setIndexWithTtl() {
        return GraphStatus(ErrorCode::E_INDEX_WITH_TTL,
                           getErrorMsg(ErrorCode::E_INDEX_WITH_TTL));
    }

    static GraphStatus setAddJobFailed() {
        return GraphStatus(ErrorCode::E_ADD_JOB_FAILED,
                           getErrorMsg(ErrorCode::E_ADD_JOB_FAILED));
    }

    static GraphStatus setStopJobFailed() {
        return GraphStatus(ErrorCode::E_STOP_JOB_FAILED,
                           getErrorMsg(ErrorCode::E_STOP_JOB_FAILED));
    }

    static GraphStatus setSaveJobFailed() {
        return GraphStatus(ErrorCode::E_SAVE_JOB_FAILED,
                           getErrorMsg(ErrorCode::E_SAVE_JOB_FAILED));
    }

    static GraphStatus setKeyHasExists() {
        return GraphStatus(ErrorCode::E_KEY_HAS_EXISTS,
                           getErrorMsg(ErrorCode::E_KEY_HAS_EXISTS));
    }

    static GraphStatus setPartNotFound() {
        return GraphStatus(ErrorCode::E_PART_NOT_FOUND,
                           getErrorMsg(ErrorCode::E_PART_NOT_FOUND));
    }

    static GraphStatus setKeyNotFound() {
        return GraphStatus(ErrorCode::E_KEY_NOT_FOUND,
                           getErrorMsg(ErrorCode::E_KEY_NOT_FOUND));
    }

    static GraphStatus setConsensusError() {
        return GraphStatus(ErrorCode::E_CONSENSUS_ERROR,
                           getErrorMsg(ErrorCode::E_CONSENSUS_ERROR));
    }

    static GraphStatus setDataTypeMismatch() {
        return GraphStatus(ErrorCode::E_DATA_TYPE_MISMATCH,
                           getErrorMsg(ErrorCode::E_DATA_TYPE_MISMATCH));
    }

    static GraphStatus setInvalidFieldValue() {
        return GraphStatus(ErrorCode::E_INVALID_FIELD_VALUE,
                           getErrorMsg(ErrorCode::E_INVALID_FIELD_VALUE));
    }

    static GraphStatus setInvalidOperation() {
        return GraphStatus(ErrorCode::E_INVALID_OPERATION,
                           getErrorMsg(ErrorCode::E_INVALID_OPERATION));
    }

    static GraphStatus setNotNullable() {
        return GraphStatus(ErrorCode::E_NOT_NULLABLE,
                           getErrorMsg(ErrorCode::E_NOT_NULLABLE));
    }

    static GraphStatus setFieldUnset() {
        return GraphStatus(ErrorCode::E_FIELD_UNSET,
                           getErrorMsg(ErrorCode::E_FIELD_UNSET));
    }

    static GraphStatus setOutOfRange() {
        return GraphStatus(ErrorCode::E_OUT_OF_RANGE,
                           getErrorMsg(ErrorCode::E_OUT_OF_RANGE));
    }

    static GraphStatus setAtomicOpFailed() {
        return GraphStatus(ErrorCode::E_ATOMIC_OP_FAILED,
                           getErrorMsg(ErrorCode::E_ATOMIC_OP_FAILED));
    }

    static GraphStatus setImproperDataType() {
        return GraphStatus(ErrorCode::E_IMPROPER_DATA_TYPE,
                           getErrorMsg(ErrorCode::E_IMPROPER_DATA_TYPE));
    }

    static GraphStatus setInvalidSpaceVidLen() {
        return GraphStatus(ErrorCode::E_INVALID_SPACEVIDLEN,
                           getErrorMsg(ErrorCode::E_INVALID_SPACEVIDLEN));
    }

    static GraphStatus setInvalidFilter() {
        return GraphStatus(ErrorCode::E_INVALID_FILTER,
                           getErrorMsg(ErrorCode::E_INVALID_FILTER));
    }

    static GraphStatus setInvalidUpdate() {
        return GraphStatus(ErrorCode::E_INVALID_UPDATER,
                           getErrorMsg(ErrorCode::E_INVALID_UPDATER));
    }

    static GraphStatus setInvalidStore() {
        return GraphStatus(ErrorCode::E_INVALID_STORE,
                           getErrorMsg(ErrorCode::E_INVALID_STORE));
    }

    static GraphStatus setInvalidPeer() {
        return GraphStatus(ErrorCode::E_INVALID_PEER,
                           getErrorMsg(ErrorCode::E_INVALID_PEER));
    }

    static GraphStatus setRetryExhausted() {
        return GraphStatus(ErrorCode::E_RETRY_EXHAUSTED,
                           getErrorMsg(ErrorCode::E_RETRY_EXHAUSTED));
    }

    static GraphStatus setTransferLeaderFailed() {
        return GraphStatus(ErrorCode::E_TRANSFER_LEADER_FAILED,
                           getErrorMsg(ErrorCode::E_TRANSFER_LEADER_FAILED));
    }

    static GraphStatus setInvalidStatType() {
        return GraphStatus(ErrorCode::E_INVALID_STAT_TYPE,
                           getErrorMsg(ErrorCode::E_INVALID_STAT_TYPE));
    }

    static GraphStatus setFailedToCheckpoint() {
        return GraphStatus(ErrorCode::E_FAILED_TO_CHECKPOINT,
                           getErrorMsg(ErrorCode::E_FAILED_TO_CHECKPOINT));
    }

    static GraphStatus setCheckpointBlocked() {
        return GraphStatus(ErrorCode::E_CHECKPOINT_BLOCKED,
                           getErrorMsg(ErrorCode::E_CHECKPOINT_BLOCKED));
    }

    static GraphStatus setFilterOut() {
        return GraphStatus(ErrorCode::E_FILTER_OUT,
                           getErrorMsg(ErrorCode::E_FILTER_OUT));
    }

    static GraphStatus setInvalidData() {
        return GraphStatus(ErrorCode::E_INVALID_DATA,
                           getErrorMsg(ErrorCode::E_INVALID_DATA));
    }

    static GraphStatus setInvalidTaskParam() {
        return GraphStatus(ErrorCode::E_INVALID_TASK_PARA,
                           getErrorMsg(ErrorCode::E_INVALID_TASK_PARA));
    }

    static GraphStatus setUserCancel() {
        return GraphStatus(ErrorCode::E_USER_CANCEL,
                           getErrorMsg(ErrorCode::E_USER_CANCEL));
    }

    static GraphStatus setSpaceNotFound(const std::string &name) {
        return GraphStatus(ErrorCode::E_SPACE_NOT_FOUND,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_SPACE_NOT_FOUND).c_str(), name.c_str()));
    }

    static GraphStatus setTagNotFound(const std::string &name) {
        return GraphStatus(ErrorCode::E_TAG_NOT_FOUND,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_TAG_NOT_FOUND).c_str(), name.c_str()));
    }

    static GraphStatus setEdgeNotFound(const std::string &name) {
        return GraphStatus(ErrorCode::E_EDGE_NOT_FOUND,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_EDGE_NOT_FOUND).c_str(), name.c_str()));
    }

    static GraphStatus setIndexNotFound(const std::string &name) {
        return GraphStatus(ErrorCode::E_INDEX_NOT_FOUND,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_INDEX_NOT_FOUND).c_str(), name.c_str()));
    }

    static GraphStatus setTagPropNotFound() {
        return GraphStatus(ErrorCode::E_TAG_PROP_NOT_FOUND,
                           getErrorMsg(ErrorCode::E_TAG_PROP_NOT_FOUND));
    }

    static GraphStatus setEdgePropNotFound() {
        return GraphStatus(ErrorCode::E_EDGE_PROP_NOT_FOUND,
                           getErrorMsg(ErrorCode::E_EDGE_PROP_NOT_FOUND));
    }

    static GraphStatus setInvalidVid() {
        return GraphStatus(ErrorCode::E_INVALID_VID,
                           getErrorMsg(ErrorCode::E_INVALID_VID));
    }

    static GraphStatus setInternalError(const std::string &reason) {
        return GraphStatus(ErrorCode::E_INTERNAL_ERROR,
                folly::stringPrintf(
                        getErrorMsg(ErrorCode::E_INTERNAL_ERROR).c_str(), reason.c_str()));
    }

public:
    ErrorCode getErrorCode() const {
        return errorCode_;
    }

    std::string toString() const {
        return errorMsg_;
    }

    friend std::ostream& operator<<(std::ostream &os, const GraphStatus &status);

    bool ok() const {
        return errorCode_ == ErrorCode::SUCCEEDED;
    }

public:
    static cpp2::Language         language;
    static cpp2::Encoding         encoding;

private:
    static cpp2::Language toLanguage(const std::string &l);

    static cpp2::Encoding toEncoding(const std::string &e);

    static std::string encode(const std::string &errorMsg);

private:
    ErrorCode        errorCode_{ErrorCode::SUCCEEDED};
    std::string                    errorMsg_;
};

inline std::ostream& operator<<(std::ostream &os, const GraphStatus &gStatus) {
    return os << gStatus.toString();
}

}   // namespace graph
}   // namespace nebula
#endif  // NEBULA_GRAPHSTATUS_H

