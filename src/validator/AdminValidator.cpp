/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "validator/AdminValidator.h"

#include "common/base/Base.h"
#include "common/charset/Charset.h"
#include "util/SchemaUtil.h"
#include "parser/MaintainSentences.h"
#include "service/GraphFlags.h"
#include "planner/Admin.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
GraphStatus CreateSpaceValidator::validateImpl() {
    auto sentence = static_cast<CreateSpaceSentence*>(sentence_);
    ifNotExist_ = sentence->isIfNotExist();
    spaceDesc_ = meta::SpaceDesc();
    spaceDesc_.spaceName_ = std::move(*(sentence->spaceName()));
    StatusOr<std::string> retStatusOr;
    std::string result;
    auto* charsetInfo = qctx_->getCharsetInfo();
    for (auto &item : sentence->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM: {
                spaceDesc_.partNum_ = item->getPartitionNum();
                if (spaceDesc_.partNum_ <= 0) {
                    return GraphStatus::setInvalidPartitionNum();
                }
                break;
            }
            case SpaceOptItem::REPLICA_FACTOR: {
                spaceDesc_.replicaFactor_ = item->getReplicaFactor();
                if (spaceDesc_.replicaFactor_ <= 0) {
                    return GraphStatus::setInvalidReplicaFactor();
                }
                break;
            }
            case SpaceOptItem::VID_SIZE: {
                spaceDesc_.vidSize_ = item->getVidSize();
                if (spaceDesc_.vidSize_ < 0 ||
                        spaceDesc_.vidSize_ > std::numeric_limits<int32_t>::max()) {
                    return GraphStatus::setInvalidSpaceVidLen();
                }
                break;
            }
            case SpaceOptItem::CHARSET: {
                result = item->getCharset();
                folly::toLowerAscii(result);
                if (!charsetInfo->isSupportCharset(result).ok()) {
                    return GraphStatus::setInvalidCharset();
                }
                spaceDesc_.charsetName_ = std::move(result);
                break;
            }
            case SpaceOptItem::COLLATE: {
                result = item->getCollate();
                folly::toLowerAscii(result);
                if (!charsetInfo->isSupportCollate(result).ok()) {
                    return GraphStatus::setInvalidCollate();
                }
                spaceDesc_.collationName_ = std::move(result);
                break;
            }
        }
    }

    // if charset and collate are not specified, set default value
    if (!spaceDesc_.charsetName_.empty() && !spaceDesc_.collationName_.empty()) {
        if (!charsetInfo->charsetAndCollateMatch(spaceDesc_.charsetName_,
                    spaceDesc_.collationName_).ok()) {
            return GraphStatus::setCharsetCollateNotMatch();
        }
    } else if (!spaceDesc_.charsetName_.empty()) {
        retStatusOr = charsetInfo->getDefaultCollationbyCharset(spaceDesc_.charsetName_);
        if (!retStatusOr.ok()) {
            LOG(ERROR) << retStatusOr.status();
            return GraphStatus::setUnsupported();
        }
        spaceDesc_.collationName_ = std::move(retStatusOr.value());
    } else if (!spaceDesc_.collationName_.empty()) {
        retStatusOr = charsetInfo->getCharsetbyCollation(spaceDesc_.collationName_);
        if (!retStatusOr.ok()) {
            LOG(ERROR) << retStatusOr.status();
            return GraphStatus::setUnsupported();
        }
        spaceDesc_.charsetName_ = std::move(retStatusOr.value());
    }

    if (spaceDesc_.charsetName_.empty() && spaceDesc_.collationName_.empty()) {
        std::string charsetName = FLAGS_default_charset;
        folly::toLowerAscii(charsetName);
        if (!charsetInfo->isSupportCharset(charsetName).ok()) {
            return GraphStatus::setUnsupported();
        }

        std::string collateName = FLAGS_default_collate;
        folly::toLowerAscii(collateName);
        if (!charsetInfo->isSupportCollate(collateName).ok()) {
            return GraphStatus::setUnsupported();
        }

        spaceDesc_.charsetName_ = std::move(charsetName);
        spaceDesc_.collationName_ = std::move(collateName);

        if (!charsetInfo->charsetAndCollateMatch(spaceDesc_.charsetName_,
                    spaceDesc_.collationName_).ok()) {
            return GraphStatus::setUnsupported();
        }
    }

    // add to validate context
    vctx_->addSpace(spaceDesc_.spaceName_);
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status CreateSpaceValidator::toPlan() {
    auto *doNode = CreateSpace::make(qctx_, nullptr, std::move(spaceDesc_), ifNotExist_);
=======
GraphStatus CreateSpaceValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto *doNode = CreateSpace::make(plan, nullptr, std::move(spaceDesc_), ifNotExist_);
>>>>>>> all use GraphStatus
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus DescSpaceValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus DescSpaceValidator::toPlan() {
    auto sentence = static_cast<DescribeSpaceSentence*>(sentence_);
    auto *doNode = DescSpace::make(qctx_, nullptr, *sentence->spaceName());
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowSpacesValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status ShowSpacesValidator::toPlan() {
    auto *doNode = ShowSpaces::make(qctx_, nullptr);
=======
GraphStatus ShowSpacesValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto *doNode = ShowSpaces::make(plan, nullptr);
>>>>>>> all use GraphStatus
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus DropSpaceValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status DropSpaceValidator::toPlan() {
=======
GraphStatus DropSpaceValidator::toPlan() {
    auto *plan = qctx_->plan();
>>>>>>> all use GraphStatus
    auto sentence = static_cast<DropSpaceSentence*>(sentence_);
    auto *doNode = DropSpace::make(qctx_, nullptr, *sentence->spaceName(), sentence->isIfExists());
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowCreateSpaceValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status ShowCreateSpaceValidator::toPlan() {
=======
GraphStatus ShowCreateSpaceValidator::toPlan() {
    auto* plan = qctx_->plan();
>>>>>>> all use GraphStatus
    auto sentence = static_cast<ShowCreateSpaceSentence*>(sentence_);
    auto spaceName = *sentence->spaceName();
    auto *doNode = ShowCreateSpace::make(qctx_, nullptr, std::move(spaceName));
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus CreateSnapshotValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status CreateSnapshotValidator::toPlan() {
    auto *doNode = CreateSnapshot::make(qctx_, nullptr);
=======
GraphStatus CreateSnapshotValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = CreateSnapshot::make(plan, nullptr);
>>>>>>> all use GraphStatus
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus DropSnapshotValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status DropSnapshotValidator::toPlan() {
=======
GraphStatus DropSnapshotValidator::toPlan() {
    auto* plan = qctx_->plan();
>>>>>>> all use GraphStatus
    auto sentence = static_cast<DropSnapshotSentence*>(sentence_);
    auto *doNode = DropSnapshot::make(qctx_, nullptr, *sentence->name());
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowSnapshotsValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status ShowSnapshotsValidator::toPlan() {
    auto *doNode = ShowSnapshots::make(qctx_, nullptr);
=======
GraphStatus ShowSnapshotsValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = ShowSnapshots::make(plan, nullptr);
>>>>>>> all use GraphStatus
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowHostsValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status ShowHostsValidator::toPlan() {
    auto *showHosts = ShowHosts::make(qctx_, nullptr);
=======
GraphStatus ShowHostsValidator::toPlan() {
    auto *showHosts = ShowHosts::make(qctx_->plan(), nullptr);
>>>>>>> all use GraphStatus
    root_ = showHosts;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowPartsValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status ShowPartsValidator::toPlan() {
=======
GraphStatus ShowPartsValidator::toPlan() {
    auto* plan = qctx_->plan();
>>>>>>> all use GraphStatus
    auto sentence = static_cast<ShowPartsSentence*>(sentence_);
    std::vector<PartitionID> partIds;
    if (sentence->getList() != nullptr) {
        partIds = *sentence->getList();
    }
    auto *node = ShowParts::make(qctx_,
                                 nullptr,
                                 vctx_->whichSpace().id,
                                 std::move(partIds));
    root_ = node;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowCharsetValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status ShowCharsetValidator::toPlan() {
    auto *node = ShowCharset::make(qctx_, nullptr);
=======
GraphStatus ShowCharsetValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *node = ShowCharset::make(plan, nullptr);
>>>>>>> all use GraphStatus
    root_ = node;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowCollationValidator::validateImpl() {
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status ShowCollationValidator::toPlan() {
    auto *node = ShowCollation::make(qctx_, nullptr);
=======
GraphStatus ShowCollationValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *node = ShowCollation::make(plan, nullptr);
>>>>>>> all use GraphStatus
    root_ = node;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowConfigsValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus ShowConfigsValidator::toPlan() {
    auto sentence = static_cast<ShowConfigsSentence*>(sentence_);
    meta::cpp2::ConfigModule module;
    auto item = sentence->configItem();
    if (item != nullptr) {
        module = item->getModule();
    } else {
        module = meta::cpp2::ConfigModule::ALL;
    }
    auto *doNode = ShowConfigs::make(qctx_, nullptr, module);
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus SetConfigValidator::validateImpl() {
    auto sentence = static_cast<SetConfigSentence*>(sentence_);
    auto item = sentence->configItem();
    if (item == nullptr) {
        return GraphStatus::setInternalError("Empty config item");
    }
    if (item->getName() != nullptr) {
        name_ = *item->getName();
    }
    name_ = *item->getName();
    module_ = item->getModule();
    auto updateItems = item->getUpdateItems();
    QueryExpressionContext ctx;
    if (updateItems == nullptr) {
        module_ = item->getModule();
        if (item->getName() != nullptr) {
            name_ = *item->getName();
        }

        if (item->getValue() != nullptr) {
            value_ = Expression::eval(item->getValue(), ctx(nullptr));
        }
    } else {
        Map configs;
        for (auto &updateItem : updateItems->items()) {
            std::string name;
            Value value;
            if (updateItem->getFieldName() == nullptr || updateItem->value() == nullptr) {
                return GraphStatus::setSemanticError("Empty config item");
            }
            name = *updateItem->getFieldName();

            value = Expression::eval(const_cast<Expression*>(updateItem->value()), ctx(nullptr));

            if (value.isNull() || (!value.isNumeric() && !value.isStr() && !value.isBool())) {
                return GraphStatus::setSemanticError(
                        folly::stringPrintf("Invalid value from `%s'", name.c_str()));
            }
            configs.kvs.emplace(std::move(name), std::move(value));
        }
        value_.setMap(std::move(configs));
    }

    return GraphStatus::OK();
}

<<<<<<< HEAD
Status SetConfigValidator::toPlan() {
    auto *doNode = SetConfig::make(qctx_,
=======
GraphStatus SetConfigValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = SetConfig::make(plan,
>>>>>>> all use GraphStatus
                                   nullptr,
                                   module_,
                                   std::move(name_),
                                   std::move(value_));
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus GetConfigValidator::validateImpl() {
    auto sentence = static_cast<GetConfigSentence*>(sentence_);
    auto item = sentence->configItem();
    if (item == nullptr) {
        return GraphStatus::setSemanticError("Empty config item");
    }

    module_ = item->getModule();
    if (item->getName() != nullptr) {
        name_ = *item->getName();
    }
    name_ = *item->getName();
    return GraphStatus::OK();
}

<<<<<<< HEAD
Status GetConfigValidator::toPlan() {
    auto *doNode = GetConfig::make(qctx_,
=======
GraphStatus GetConfigValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = GetConfig::make(plan,
>>>>>>> all use GraphStatus
                                   nullptr,
                                   module_,
                                   std::move(name_));
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}
}  // namespace graph
}  // namespace nebula
