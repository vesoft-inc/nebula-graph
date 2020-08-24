/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/interface/gen-cpp2/common_types.h"

#include "util/GraphStatus.h"
#include <boost/locale/info.hpp>
#include <boost/locale/util.hpp>
#include <boost/compute/detail/getenv.hpp>

namespace nebula {
namespace graph {

cpp2::Language GraphStatus::language = cpp2::Language::L_EN;
cpp2::Encoding GraphStatus::encoding = cpp2::Encoding::E_UTF8;

void GraphStatus::init() {
    auto sysLocale = boost::locale::util::get_system_locale(false);
    std::locale locale = boost::locale::util::create_info(std::locale::classic(), sysLocale);
    boost::locale::info const &info = std::use_facet<boost::locale::info>(locale);
    language = toLanguage(info.language());
    encoding = toEncoding(info.encoding());
}

GraphStatus GraphStatus::setRpcResponse(const nebula::cpp2::ErrorCode code,
                                        const std::string &name) {
    switch (code) {
        case nebula::cpp2::ErrorCode::SUCCEEDED:
            return GraphStatus::OK();
        case nebula::cpp2::ErrorCode::E_RPC_FAILED:
            return GraphStatus::setRpcFailed(name);
        case nebula::cpp2::ErrorCode::E_LEADER_CHANGED:
            return GraphStatus::setLeaderChanged();
        case nebula::cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD:
            return GraphStatus::setBadUsernamePassword();
        case nebula::cpp2::ErrorCode::E_NO_HOSTS:
            return GraphStatus::setNoHosts();
        case nebula::cpp2::ErrorCode::E_SPACE_EXISTED:
            return GraphStatus::setSpaceExisted(name);
        case nebula::cpp2::ErrorCode::E_TAG_EXISTED:
            return GraphStatus::setTagExisted(name);
        case nebula::cpp2::ErrorCode::E_EDGE_EXISTED:
            return GraphStatus::setEdgeExisted(name);
        case nebula::cpp2::ErrorCode::E_INDEX_EXISTED:
            return GraphStatus::setIndexExisted(name);
        case nebula::cpp2::ErrorCode::E_UNSUPPORTED:
            return GraphStatus::setUnsupported();
        case nebula::cpp2::ErrorCode::E_NOT_DROP_PROP:
            return GraphStatus::setNotDropProp();
        case nebula::cpp2::ErrorCode::E_CONFIG_IMMUTABLE:
            return GraphStatus::setConfigImmutable(name);
        case nebula::cpp2::ErrorCode::E_CONFLICT:
            return GraphStatus::setConflict(name);
        case nebula::cpp2::ErrorCode::E_INVALID_PARAM:
            return GraphStatus::setInvalidParam(name);
        case nebula::cpp2::ErrorCode::E_STORE_FAILED:
            return GraphStatus::setStoreFailed();
        case nebula::cpp2::ErrorCode::E_STORE_SEGMENT_ILLEGAL:
            return GraphStatus::setStoreSegmentIllegal();
        case nebula::cpp2::ErrorCode::E_BALANCER_RUNNING:
            return GraphStatus::setBalancerRunning();
        case nebula::cpp2::ErrorCode::E_BAD_BALANCE_PLAN:
            return GraphStatus::setBadBalancePlan();
        case nebula::cpp2::ErrorCode::E_BALANCED:
            return GraphStatus::setBalanced();
        case nebula::cpp2::ErrorCode::E_NO_RUNNING_BALANCE_PLAN:
            return GraphStatus::setNoRunningBalancePlan();
        case nebula::cpp2::ErrorCode::E_CORRUPTTED_BALANCE_PLAN:
            return GraphStatus::setCorrupttedBalancePlan();
        case nebula::cpp2::ErrorCode::E_NO_VALID_HOST:
            return GraphStatus::setNoValidHost();
        case nebula::cpp2::ErrorCode::E_INVALID_PASSWORD:
            return GraphStatus::setInvalidPassword();
        case nebula::cpp2::ErrorCode::E_IMPROPER_ROLE:
            return GraphStatus::setImproperRole();
        case nebula::cpp2::ErrorCode::E_INVALID_PARTITION_NUM:
            return GraphStatus::setInvalidPartitionNum();
        case nebula::cpp2::ErrorCode::E_INVALID_REPLICA_FACTOR:
            return GraphStatus::setInvalidReplicaFactor();
        case nebula::cpp2::ErrorCode::E_INVALID_CHARSET:
            return GraphStatus::setInvalidCharset();
        case nebula::cpp2::ErrorCode::E_INVALID_COLLATE:
            return GraphStatus::setInvalidCollate();
        case nebula::cpp2::ErrorCode::E_CHARSET_COLLATE_NOT_MATCH:
            return GraphStatus::setCharsetCollateNotMatch();
        case nebula::cpp2::ErrorCode::E_SNAPSHOT_FAILED:
            return GraphStatus::setSnapshotFailed();
        case nebula::cpp2::ErrorCode::E_BLOCK_WRITE_FAILED:
            return GraphStatus::setBlockWriteFailed();
        case nebula::cpp2::ErrorCode::E_REBUILD_INDEX_FAILED:
            return GraphStatus::setRebuildIndexFailed();
        case nebula::cpp2::ErrorCode::E_INDEX_WITH_TTL:
            return GraphStatus::setIndexWithTtl();
        case nebula::cpp2::ErrorCode::E_ADD_JOB_FAILED:
            return GraphStatus::setAddJobFailed();
        case nebula::cpp2::ErrorCode::E_STOP_JOB_FAILED:
            return GraphStatus::setStopJobFailed();
        case nebula::cpp2::ErrorCode::E_SAVE_JOB_FAILED:
            return GraphStatus::setSaveJobFailed();
        case nebula::cpp2::ErrorCode::E_KEY_HAS_EXISTS:
            return GraphStatus::setKeyHasExists();
        case nebula::cpp2::ErrorCode::E_PART_NOT_FOUND:
            return GraphStatus::setPartNotFound();
        case nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND:
            return GraphStatus::setKeyNotFound();
        case nebula::cpp2::ErrorCode::E_CONSENSUS_ERROR:
            return GraphStatus::setConsensusError();
        case nebula::cpp2::ErrorCode::E_DATA_TYPE_MISMATCH:
            return GraphStatus::setDataTypeMismatch();
        case nebula::cpp2::ErrorCode::E_INVALID_FIELD_VALUE:
            return GraphStatus::setInvalidFieldValue();
        case nebula::cpp2::ErrorCode::E_INVALID_OPERATION:
            return GraphStatus::setInvalidOperation();
        case nebula::cpp2::ErrorCode::E_NOT_NULLABLE:
            return GraphStatus::setNotNullable();
        case nebula::cpp2::ErrorCode::E_FIELD_UNSET:
            return GraphStatus::setFieldUnset();
        case nebula::cpp2::ErrorCode::E_OUT_OF_RANGE:
            return GraphStatus::setOutOfRange();
        case nebula::cpp2::ErrorCode::E_ATOMIC_OP_FAILED:
            return GraphStatus::setAtomicOpFailed();
        case nebula::cpp2::ErrorCode::E_IMPROPER_DATA_TYPE:
            return GraphStatus::setImproperDataType();
        case nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN:
            return GraphStatus::setInvalidSpaceVidLen();
        case nebula::cpp2::ErrorCode::E_INVALID_FILTER:
            return GraphStatus::setInvalidFilter();
        case nebula::cpp2::ErrorCode::E_INVALID_UPDATER:
            return GraphStatus::setInvalidFilter();
        case nebula::cpp2::ErrorCode::E_INVALID_STORE:
            return GraphStatus::setInvalidStore();
        case nebula::cpp2::ErrorCode::E_INVALID_PEER:
            return GraphStatus::setInvalidPeer();
        case nebula::cpp2::ErrorCode::E_RETRY_EXHAUSTED:
            return GraphStatus::setRetryExhausted();
        case nebula::cpp2::ErrorCode::E_TRANSFER_LEADER_FAILED:
            return GraphStatus::setTransferLeaderFailed();
        case nebula::cpp2::ErrorCode::E_INVALID_STAT_TYPE:
            return GraphStatus::setInvalidStatType();
        case nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT:
            return GraphStatus::setFailedToCheckpoint();
        case nebula::cpp2::ErrorCode::E_CHECKPOINT_BLOCKED:
            return GraphStatus::setCheckpointBlocked();
        case nebula::cpp2::ErrorCode::E_FILTER_OUT:
            return GraphStatus::setFilterOut();
        case nebula::cpp2::ErrorCode::E_INVALID_DATA:
            return GraphStatus::setInvalidData();
        case nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA:
            return GraphStatus::setInvalidTaskParam();
        case nebula::cpp2::ErrorCode::E_USER_CANCEL:
            return GraphStatus::setUserCancel();
        case nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND:
            return GraphStatus::setSpaceNotFound(name);
        case nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND:
            return GraphStatus::setTagNotFound(name);
        case nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND:
            return GraphStatus::setEdgeNotFound(name);
        case nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND:
            return GraphStatus::setIndexNotFound(name);
        case nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND:
            return GraphStatus::setTagPropNotFound();
        case nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND:
            return GraphStatus::setEdgePropNotFound();
        case nebula::cpp2::ErrorCode::E_INVALID_VID:
            return GraphStatus::setInvalidVid();
        case nebula::cpp2::ErrorCode::E_INTERNAL_ERROR:
            return GraphStatus::setInternalError(name);
        default:
            return GraphStatus::setInternalError(
                    folly::stringPrintf("Unprocessed error code `%s'",
                                         nebula::cpp2::_ErrorCode_VALUES_TO_NAMES.at(code)));
    }
}

cpp2::Language GraphStatus::toLanguage(const std::string &l) {
    if (l == "en" || l == "c") {
        return cpp2::Language::L_EN;
    }
    LOG(ERROR) << "Not supported language: " << l;
    return cpp2::Language::L_EN;
}

cpp2::Encoding GraphStatus::toEncoding(const std::string &e) {
    if (e == "utf-8") {
        return cpp2::Encoding::E_UTF8;
    }
    LOG(ERROR) << "Not supported encoding: " << e;
    return cpp2::Encoding::E_UTF8;
}

std::string GraphStatus::encode(const std::string &errorMsg) {
    return errorMsg;
}

}  // namespace graph
}  // namespace nebula

