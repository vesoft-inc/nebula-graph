/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "base/Base.h"
#include "charset/Charset.h"

#include "util/SchemaCommon.h"
#include "parser/MaintainSentences.h"
#include "service/GraphFlags.h"
#include "planner/Maintain.h"
#include "planner/Query.h"
#include "CreateValidator.h"

namespace nebula {
namespace graph {
Status CreateSpaceValidator::validateImpl() {
    auto status = Status::OK();
    spaceDesc_ = meta::SpaceDesc();
    spaceDesc_.spaceName_ = std::move(*(sentence_->spaceName()));
    Status retStatus;
    StatusOr<std::string> retStatusOr;
    std::string result;
    auto* charsetInfo = validateContext_->getCharsetInfo();
    for (auto &item : sentence_->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM: {
                spaceDesc_.partNum_ = item->getPartitionNum();
                if (spaceDesc_.partNum_ <= 0) {
                    return Status::Error("Partition_num value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::REPLICA_FACTOR: {
                spaceDesc_.replicaFactor_ = item->getReplicaFactor();
                if (spaceDesc_.replicaFactor_ <= 0) {
                    return Status::Error("Replica_factor value should be greater than zero");
                }
                break;
            }
            case SpaceOptItem::VID_SIZE: {
                spaceDesc_.vidSize_ = item->getVidSize();
                if (spaceDesc_.vidSize_ < 0 ||
                        spaceDesc_.vidSize_ > std::numeric_limits<int32_t>::max()) {
                    return Status::Error("Vid_size value is incorrect");
                }
                break;
            }
            case SpaceOptItem::CHARSET: {
                result = item->getCharset();
                folly::toLowerAscii(result);
                retStatus = charsetInfo->isSupportCharset(result);
                if (!retStatus.ok()) {
                    return retStatus;
                }
                spaceDesc_.charsetName_ = std::move(result);
                break;
            }
            case SpaceOptItem::COLLATE: {
                result = item->getCollate();
                folly::toLowerAscii(result);
                retStatus = charsetInfo->isSupportCollate(result);
                if (!retStatus.ok()) {
                    return retStatus;
                }
                spaceDesc_.collationName_ = std::move(result);
                break;
            }
        }
    }

    // if charset and collate are not specified, set default value
    if (!spaceDesc_.charsetName_.empty() && !spaceDesc_.collationName_.empty()) {
        retStatus = charsetInfo->charsetAndCollateMatch(spaceDesc_.charsetName_,
                                                        spaceDesc_.collationName_);
        if (!retStatus.ok()) {
            return retStatus;
        }
    } else if (!spaceDesc_.charsetName_.empty()) {
        retStatusOr = charsetInfo->getDefaultCollationbyCharset(spaceDesc_.charsetName_);
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceDesc_.collationName_ = std::move(retStatusOr.value());
    } else if (!spaceDesc_.collationName_.empty()) {
        retStatusOr = charsetInfo->getCharsetbyCollation(spaceDesc_.collationName_);
        if (!retStatusOr.ok()) {
            return retStatusOr.status();
        }
        spaceDesc_.charsetName_ = std::move(retStatusOr.value());
    }

    if (spaceDesc_.charsetName_.empty() && spaceDesc_.collationName_.empty()) {
        std::string charsetName = FLAGS_default_charset;
        folly::toLowerAscii(charsetName);
        retStatus = charsetInfo->isSupportCharset(charsetName);
        if (!retStatus.ok()) {
            return retStatus;
        }

        std::string collateName = FLAGS_default_collate;
        folly::toLowerAscii(collateName);
        retStatus = charsetInfo->isSupportCollate(collateName);
        if (!retStatus.ok()) {
            return retStatus;
        }

        spaceDesc_.charsetName_ = std::move(charsetName);
        spaceDesc_.collationName_ = std::move(collateName);

        retStatus = charsetInfo->charsetAndCollateMatch(spaceDesc_.charsetName_,
                                                        spaceDesc_.collationName_);
        if (!retStatus.ok()) {
            return retStatus;
        }
    }

    ifNotExist_ = sentence_->isIfNotExist();
    return status;
}

Status CreateSpaceValidator::toPlan() {
    auto* plan = validateContext_->plan();
    root_ = StartNode::make(plan);
    auto *doNode = CreateSpace::make(plan,
                                     spaceDesc_,
                                     ifNotExist_);
    YieldColumns* cols = nullptr;
    auto *project = Project::make(plan, doNode, cols);
    plan->setRoot(project);
    return Status::OK();
}

Status CreateTagValidator::validateImpl() {
    auto status = Status::OK();
    tagName_ = *sentence_->name();
    ifNotExist_ = sentence_->isIfNotExist();
    do {
        status = SchemaCommon::validateColumns(sentence_->columnSpecs(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
        status = SchemaCommon::validateProps(sentence_->getSchemaProps(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
    } while (false);
    return status;
}

Status CreateTagValidator::toPlan() {
    auto* plan = validateContext_->plan();
    root_ = StartNode::make(plan);
    auto *doNode = CreateTag::make(plan,
                                   validateContext_->whichSpace().id,
                                   tagName_,
                                   schema_,
                                   ifNotExist_);
    YieldColumns* cols = nullptr;
    auto *project = Project::make(plan, doNode, cols);
    plan->setRoot(project);
    return Status::OK();
}

Status CreateEdgeValidator::validateImpl() {
    auto status = Status::OK();
    edgeName_ = *sentence_->name();
    ifNotExist_ = sentence_->isIfNotExist();
    do {
        status = SchemaCommon::validateColumns(sentence_->columnSpecs(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
        status = SchemaCommon::validateProps(sentence_->getSchemaProps(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
    } while (false);
    return status;
}

Status CreateEdgeValidator::toPlan() {
    auto* plan = validateContext_->plan();
    root_ = StartNode::make(plan);
    auto *doNode = CreateTag::make(plan,
                                   validateContext_->whichSpace().id,
                                   edgeName_,
                                   schema_,
                                   ifNotExist_);
    YieldColumns* cols = nullptr;
    auto *project = Project::make(plan, doNode, cols);
    plan->setRoot(project);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
