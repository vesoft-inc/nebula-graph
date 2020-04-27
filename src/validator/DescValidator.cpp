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
#include "DescValidator.h"

namespace nebula {
namespace graph {
Status DescSpaceValidator::validateImpl() {
    spaceName_ = *sentence_->spaceName();
    return Status::OK();
}

Status DescSpaceValidator::toPlan() {
    auto* plan = validateContext_->plan();
    root_ = StartNode::make(plan);
    auto *doNode = DescSpace::make(plan, spaceName_);
    YieldColumns* cols = nullptr;
    auto *project = Project::make(plan, doNode, cols);
    root_ = project;
    tail_ = root_;
    return Status::OK();
}

Status DescTagValidator::validateImpl() {
    tagName_ = *sentence_->name();
    return Status::OK();
}

Status DescTagValidator::toPlan() {
    auto* plan = validateContext_->plan();
    root_ = StartNode::make(plan);
    auto *doNode = DescTag::make(plan,
                                 validateContext_->whichSpace().id,
                                 tagName_);
    YieldColumns* cols = nullptr;
    auto *project = Project::make(plan, doNode, cols);
    root_ = project;
    tail_ = root_;
    return Status::OK();
}

Status DescEdgeValidator::validateImpl() {
    edgeName_ = *sentence_->name();
    return Status::OK();
}

Status DescEdgeValidator::toPlan() {
    auto* plan = validateContext_->plan();
    root_ = StartNode::make(plan);
    auto *doNode = DescTag::make(plan,
                                 validateContext_->whichSpace().id,
                                 edgeName_);
    YieldColumns* cols = nullptr;
    auto *project = Project::make(plan, doNode, cols);
    root_ = project;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
