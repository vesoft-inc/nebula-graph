/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "base/Base.h"
#include "charset/Charset.h"

#include "util/SchemaUtil.h"
#include "parser/MaintainSentences.h"
#include "service/GraphFlags.h"
#include "planner/Maintain.h"
#include "planner/Query.h"
#include "validator/MaintainValidator.h"

namespace nebula {
namespace graph {
Status CreateTagValidator::validateImpl() {
    auto status = Status::OK();
    tagName_ = *sentence_->name();
    ifNotExist_ = sentence_->isIfNotExist();
    do {
        status = SchemaUtil::validateColumns(sentence_->columnSpecs(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
        status = SchemaUtil::validateProps(sentence_->getSchemaProps(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
    } while (false);
    return status;
}

Status CreateTagValidator::toPlan() {
    auto* plan = validateContext_->plan();
    auto *doNode = CreateTagNode::make(plan,
                                   validateContext_->whichSpace().id,
                                   tagName_,
                                   schema_,
                                   ifNotExist_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status CreateEdgeValidator::validateImpl() {
    auto status = Status::OK();
    edgeName_ = *sentence_->name();
    ifNotExist_ = sentence_->isIfNotExist();
    do {
        status = SchemaUtil::validateColumns(sentence_->columnSpecs(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
        status = SchemaUtil::validateProps(sentence_->getSchemaProps(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
    } while (false);
    return status;
}

Status CreateEdgeValidator::toPlan() {
    auto* plan = validateContext_->plan();
    auto *doNode = CreateEdgeNode::make(plan,
                                    validateContext_->whichSpace().id,
                                    edgeName_,
                                    schema_,
                                    ifNotExist_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status DescTagValidator::validateImpl() {
    tagName_ = *sentence_->name();
    return Status::OK();
}

Status DescTagValidator::toPlan() {
    auto* plan = validateContext_->plan();
    auto *doNode = DescTagNode::make(plan,
                                 validateContext_->whichSpace().id,
                                 tagName_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status DescEdgeValidator::validateImpl() {
    edgeName_ = *sentence_->name();
    return Status::OK();
}

Status DescEdgeValidator::toPlan() {
    auto* plan = validateContext_->plan();
    auto *doNode = DescEdgeNode::make(plan,
                                  validateContext_->whichSpace().id,
                                  edgeName_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
