/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"
#include "common/charset/Charset.h"

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
        // Check the validateContext has the same name schema
        auto pro = vctx_->getSchema(tagName_);
        if (pro != nullptr) {
            status = Status::Error("Has the same name schema of `%s'", tagName_.c_str());
            break;
        }

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
    // Save the schema in validateContext
    if (status.ok()) {
        auto schemaPro = SchemaUtil::generateSchemaProvider(0, schema_);
        vctx_->addSchema(tagName_, schemaPro);
    }
    return status;
}

Status CreateTagValidator::toPlan() {
<<<<<<< HEAD
    auto* plan = qctx_->plan();
    auto *doNode = CreateTag::make(plan,
                                   nullptr,
                                   vctx_->whichSpace().id,
                                   tagName_,
                                   schema_,
                                   ifNotExist_);
=======
    auto *plan = qctx_->plan();
    CreateTag* doNode = nullptr;
    if (plan->empty()) {
        auto *start = StartNode::make(plan);
        doNode = CreateTag::make(plan,
                start, std::move(tagName_), std::move(schema_), ifNotExist_);
        root_ = doNode;
        tail_ = start;
    } else {
        doNode = CreateTag::make(plan,
                plan->root(), std::move(tagName_), std::move(schema_), ifNotExist_);
        root_ = doNode;
        tail_ = root_;
    }
<<<<<<< HEAD
>>>>>>> Support DML,DDL to use inputNode
    root_ = doNode;
    tail_ = root_;
=======
>>>>>>> address comment
    return Status::OK();
}

Status CreateEdgeValidator::validateImpl() {
    auto status = Status::OK();
    edgeName_ = *sentence_->name();
    ifNotExist_ = sentence_->isIfNotExist();
    do {
        // Check the validateContext has the same name schema
        auto pro = vctx_->getSchema(edgeName_);
        if (pro != nullptr) {
            status = Status::Error("Has the same name schema of `%s'", edgeName_.c_str());
            break;
        }

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

    // Save the schema in validateContext
    if (status.ok()) {
        auto schemaPro = SchemaUtil::generateSchemaProvider(0, schema_);
        vctx_->addSchema(edgeName_, schemaPro);
    }
    return status;
}

Status CreateEdgeValidator::toPlan() {
<<<<<<< HEAD
    auto* plan = qctx_->plan();
    auto *doNode = CreateEdge::make(plan,
                                    nullptr,
                                    vctx_->whichSpace().id,
                                    edgeName_,
                                    schema_,
                                    ifNotExist_);
=======
    auto *plan = qctx_->plan();
    CreateEdge* doNode = nullptr;
    if (plan->empty()) {
        auto *start = StartNode::make(plan);
        doNode = CreateEdge::make(plan,
                start, std::move(edgeName_), std::move(schema_), ifNotExist_);
        root_ = doNode;
        tail_ = start;
    } else {
        doNode = CreateEdge::make(plan,
                plan->root(), std::move(edgeName_), std::move(schema_), ifNotExist_);
        root_ = doNode;
        tail_ = root_;
    }
<<<<<<< HEAD
>>>>>>> Support DML,DDL to use inputNode
    root_ = doNode;
    tail_ = root_;
=======
>>>>>>> address comment
    return Status::OK();
}

Status DescTagValidator::validateImpl() {
    return Status::OK();
}

Status DescTagValidator::toPlan() {
<<<<<<< HEAD
    auto* plan = qctx_->plan();
    auto *doNode = DescTag::make(plan,
                                 nullptr,
                                 vctx_->whichSpace().id,
                                 tagName_);
=======
    auto sentence = static_cast<DescribeTagSentence*>(sentence_);
    auto name = *sentence->name();
    auto *plan = qctx_->plan();
<<<<<<< HEAD
    DescTag* doNode = nullptr;
    if (plan->empty()) {
        auto *start = StartNode::make(plan);
        doNode = DescTag::make(plan, start, std::move(name));
        root_ = doNode;
        tail_ = start;
    } else {
        doNode = DescTag::make(plan, plan->root(), std::move(name));
        root_ = doNode;
        tail_ = root_;
    }
<<<<<<< HEAD
>>>>>>> Support DML,DDL to use inputNode
    root_ = doNode;
    tail_ = root_;
=======
>>>>>>> address comment
=======
    auto doNode = DescTag::make(plan, nullptr, std::move(name));
    root_ = doNode;
    tail_ = root_;
>>>>>>> rebase upstream
    return Status::OK();
}

Status DescEdgeValidator::validateImpl() {
    return Status::OK();
}

Status DescEdgeValidator::toPlan() {
<<<<<<< HEAD
    auto* plan = qctx_->plan();
    auto *doNode = DescEdge::make(plan,
                                  nullptr,
                                  vctx_->whichSpace().id,
                                  edgeName_);
=======
    auto sentence = static_cast<DescribeEdgeSentence*>(sentence_);
    auto name = *sentence->name();
    auto *plan = qctx_->plan();
<<<<<<< HEAD
    DescEdge* doNode = nullptr;
    if (plan->empty()) {
        auto *start = StartNode::make(plan);
        doNode = DescEdge::make(plan, start, std::move(name));
        root_ = doNode;
        tail_ = start;
    } else {
        doNode = DescEdge::make(plan, plan->root(), std::move(name));
        root_ = doNode;
        tail_ = root_;
    }
<<<<<<< HEAD
>>>>>>> Support DML,DDL to use inputNode
    root_ = doNode;
    tail_ = root_;
=======
>>>>>>> address comment
=======
    auto doNode = DescEdge::make(plan, nullptr, std::move(name));
    root_ = doNode;
    tail_ = root_;
>>>>>>> rebase upstream
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
