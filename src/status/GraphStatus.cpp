/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "GraphStatus.h"
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

template<typename RESP>
GraphStatus GraphStatus::setMetaResponse(const RESP& resp,
                                         const std::string &name,
                                         const TypeName type) {
    switch (resp.get_code()) {
        case meta::cpp2::ErrorCode::SUCCEEDED:
            return GraphStatus::OK();
        case meta::cpp2::ErrorCode::E_NO_HOSTS:
            return GraphStatus::setNoHosts();
        case meta::cpp2::ErrorCode::E_EXISTED: {
            if (type == TypeName::T_SPACE) {
                return GraphStatus::setSpaceExisted(name);
            } else if (type == TypeName::T_TAG) {
                return GraphStatus::setTagExisted(name);
            } else if (type == TypeName::T_EDGE) {
                return GraphStatus::setEdgeExisted(name);
            } else if (type == TypeName::T_INDEX) {
                return GraphStatus::setIndexExisted(name);
            }
        }
        case meta::cpp2::ErrorCode::E_NOT_FOUND: {
            if (type == TypeName::T_SPACE) {
                return GraphStatus::setSpaceNotFound(name);
            } else if (type == TypeName::T_TAG) {
                return GraphStatus::setTagNotFound(name);
            } else if (type == TypeName::T_EDGE) {
                return GraphStatus::setEdgeNotFound(name);
            } else if (type == TypeName::T_INDEX) {
                return GraphStatus::setIndexNotFound(name);
            }
        }
        case meta::cpp2::ErrorCode::E_CONFLICT: {
            if (type == TypeName::T_TAG) {
                return GraphStatus::setTagExisted(name);
            } else if (type == TypeName::T_EDGE) {
                return GraphStatus::setEdgeExisted(name);
            }
        }
        case meta::cpp2::ErrorCode::E_CONFIG_IMMUTABLE:
            return GraphStatus::setConfigImmutable(name);
        case meta::cpp2::ErrorCode::E_LEADER_CHANGED:
            return GraphStatus::setMetaLeaderChange();
        case meta::cpp2::ErrorCode::E_BALANCED:
            return GraphStatus::setBalanced();
        case meta::cpp2::ErrorCode::E_BALANCER_RUNNING:
            return GraphStatus::setBalancerRunning();
        case meta::cpp2::ErrorCode::E_BAD_BALANCE_PLAN:
            return GraphStatus::setBadBalancePlan();
        case meta::cpp2::ErrorCode::E_NO_RUNNING_BALANCE_PLAN:
            return GraphStatus::setNoRunningBalancePlan();
        case meta::cpp2::ErrorCode::E_NO_VALID_HOST:
            return GraphStatus::setNoValidHost();
        case meta::cpp2::ErrorCode::E_CORRUPTTED_BALANCE_PLAN:
            return GraphStatus::setCorrupttedBlancePlan();
        case meta::cpp2::ErrorCode::E_INVALID_PARTITION_NUM:
        case meta::cpp2::ErrorCode::E_INVALID_REPLICA_FACTOR:
        case meta::cpp2::ErrorCode::E_INVALID_CHARSET:
        case meta::cpp2::ErrorCode::E_INVALID_COLLATE:
        case meta::cpp2::ErrorCode::E_CHARSET_COLLATE_NOT_MATCH:
            return GraphStatus::setInvalidParam(name);
        default:
            return GraphStatus::setInternalError("Unknown meta error code");
    }
    return GraphStatus::OK();
}

template<typename RESP>
GraphStatus GraphStatus::setStorageResponse(const RESP& resp, const std::string &name) {
    switch (resp.get_code()) {
        case storage::cpp2::ErrorCode::E_LEADER_CHANGED:
            return GraphStatus::setLeaderChange();
        case storage::cpp2::ErrorCode::E_KEY_HAS_EXISTS:
            return GraphStatus::setKeyHasExists();
        case storage::cpp2::ErrorCode::E_SPACE_NOT_FOUND:
            return GraphStatus::setSpaceNotFound(name);
        case storage::cpp2::ErrorCode::E_PART_NOT_FOUND:
            return GraphStatus::setPartNotFound();
        case storage::cpp2::ErrorCode::E_KEY_NOT_FOUND:
            return GraphStatus::setKeyNotFound();
        case storage::cpp2::ErrorCode::E_CONSENSUS_ERROR:
            return GraphStatus::setConsensusError();
        case storage::cpp2::ErrorCode::E_IMPROPER_DATA_TYPE:
            return GraphStatus::setImproperDataType();
        case storage::cpp2::ErrorCode::E_EDGE_NOT_FOUND:
            return GraphStatus::setEdgeNotFound(name);
        case storage::cpp2::ErrorCode::E_TAG_NOT_FOUND:
            return GraphStatus::setTagNotFound(name);
        case storage::cpp2::ErrorCode::E_INVALID_FILTER:
        case storage::cpp2::ErrorCode::E_INVALID_UPDATER:
        case storage::cpp2::ErrorCode::E_INVALID_STORE:
        case storage::cpp2::ErrorCode::E_INVALID_PEER:
            return GraphStatus::setInvalidParam(name);
        case storage::cpp2::ErrorCode::E_RETRY_EXHAUSTED:
        case storage::cpp2::ErrorCode::E_TRANSFER_LEADER_FAILED:
        case storage::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT:
            return GraphStatus::setFailedToCheckpoint();
        case storage::cpp2::ErrorCode::E_CHECKPOINT_BLOCKED:
            return GraphStatus::setCheckpointBlocked();
        default:
            return GraphStatus::setInternalError("Unknown storage error code");
    }

    return GraphStatus::OK();
}

cpp2::Language GraphStatus::toLanguage(const std::string &language) {
    if (language == "en" || language == "c") {
        return cpp2::Language::L_EN;
    }
    LOG(ERROR) << "Not supported language: " << language;
    return cpp2::Language::L_EN;
}

cpp2::Encoding GraphStatus::toEncoding(const std::string &encoding) {
    if (encoding == "utf-8") {
        return cpp2::Encoding::E_UTF8;
    }
    LOG(ERROR) << "Not supported encoding: " << encoding;
    return cpp2::Encoding::E_UTF8;
}

std::string GraphStatus::encode(const std::string &errorMsg) {
    return errorMsg;
}

}  // namespace graph
}  // namespace nebula
