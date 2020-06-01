/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/AdminSentences.h"

namespace nebula {

std::string ShowSentence::toString() const {
    switch (showType_) {
        case ShowType::kShowHosts:
            return std::string("SHOW HOSTS");
        case ShowType::kShowSpaces:
            return std::string("SHOW SPACES");
        case ShowType::kShowParts:
            return std::string("SHOW PARTS");
        case ShowType::kShowTags:
            return std::string("SHOW TAGS");
        case ShowType::kShowEdges:
            return std::string("SHOW EDGES");
        case ShowType::kShowUsers:
            return std::string("SHOW USERS");
        case ShowType::kShowRoles:
            return folly::stringPrintf("SHOW ROLES IN %s", name_.get()->c_str());
        case ShowType::kShowCreateSpace:
            return folly::stringPrintf("SHOW CREATE SPACE %s", name_.get()->c_str());
        case ShowType::kShowCreateTag:
            return folly::stringPrintf("SHOW CREATE TAG %s", name_.get()->c_str());
        case ShowType::kShowCreateEdge:
            return folly::stringPrintf("SHOW CREATE EDGE %s", name_.get()->c_str());
        case ShowType::kShowSnapshots:
            return folly::stringPrintf("SHOW SNAPSHOTS");
        case ShowType::kShowCharset:
            return folly::stringPrintf("SHOW CHARSET");
        case ShowType::kShowCollation:
            return folly::stringPrintf("SHOW COLLATION");
        case ShowType::kUnknown:
        default:
            FLOG_FATAL("Type illegal");
    }
    return "Unknown";
}


std::string SpaceOptItem::toString() const {
    switch (optType_) {
        case PARTITION_NUM:
            return folly::stringPrintf("partition_num = %ld", boost::get<int64_t>(optValue_));
        case REPLICA_FACTOR:
            return folly::stringPrintf("replica_factor = %ld", boost::get<int64_t>(optValue_));
        case VID_SIZE:
            return folly::stringPrintf("vid_size = %ld", boost::get<int64_t>(optValue_));
        case CHARSET:
            return folly::stringPrintf("charset = %s", boost::get<std::string>(optValue_).c_str());
        case COLLATE:
            return folly::stringPrintf("collate = %s", boost::get<std::string>(optValue_).c_str());
        default:
             FLOG_FATAL("Space parameter illegal");
    }
    return "Unknown";
}


std::string SpaceOptList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : items_) {
        buf += item->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size()-1);
    }
    return buf;
}


std::string CreateSpaceSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE SPACE ";
    buf += *spaceName_;
    if (spaceOpts_ != nullptr) {
        buf += "(";
        buf += spaceOpts_->toString();
        buf += ")";
    }
    return buf;
}


std::string DropSpaceSentence::toString() const {
    return folly::stringPrintf("DROP SPACE %s", spaceName_.get()->c_str());
}


std::string DescribeSpaceSentence::toString() const {
    return folly::stringPrintf("DESCRIBE SPACE %s", spaceName_.get()->c_str());
}

std::string ConfigRowItem::toString() const {
    return "";
}

std::string ConfigSentence::toString() const {
    switch (subType_) {
        case SubType::kShow:
            return std::string("SHOW CONFIGS ") + configItem_->toString();
        case SubType::kSet:
            return std::string("SET CONFIGS ") + configItem_->toString();
        case SubType::kGet:
            return std::string("GET CONFIGS ") + configItem_->toString();
        default:
            FLOG_FATAL("Type illegal");
    }
    return "Unknown";
}

std::string BalanceSentence::toString() const {
    switch (subType_) {
        case SubType::kLeader:
            return std::string("BALANCE LEADER");
        default:
            FLOG_FATAL("Type illegal");
    }
    return "Unknown";
}

std::string HostList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &host : hosts_) {
        buf += host->host;
        buf += ":";
        buf += std::to_string(host->port);
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}

std::string CreateSnapshotSentence::toString() const {
    return "CREATE SNAPSHOT";
}

std::string DropSnapshotSentence::toString() const {
    return folly::stringPrintf("DROP SNAPSHOT %s", name_.get()->c_str());
}

std::string AdminJobSentence::toString() const {
    switch (op_) {
    case meta::cpp2::AdminJobOp::ADD:
        return "add_job";
    case meta::cpp2::AdminJobOp::SHOW_All:
        return "show_jobs";
    case meta::cpp2::AdminJobOp::SHOW:
        return "show_job";
    case meta::cpp2::AdminJobOp::STOP:
        return "stop job";
    case meta::cpp2::AdminJobOp::RECOVER:
        return "recover job";
    }
    LOG(FATAL) << "Unkown job operation " << static_cast<uint8_t>(op_);
}

meta::cpp2::AdminJobOp AdminJobSentence::getType() const {
    return op_;
}

std::vector<std::string> AdminJobSentence::getParas() const {
    return paras_;
}

void AdminJobSentence::addPara(const std::string& para) {
    paras_.emplace_back(para);
}

}   // namespace nebula
