/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/Validator.h"

#include "parser/Sentence.h"
#include "planner/Query.h"
#include "validator/GoValidator.h"
#include "validator/PipeValidator.h"
#include "validator/ReportError.h"
#include "validator/SequentialValidator.h"
#include "validator/AssignmentValidator.h"
#include "validator/SetValidator.h"
#include "validator/UseValidator.h"
#include "validator/GetSubgraphValidator.h"
#include "validator/AdminValidator.h"
#include "validator/MaintainValidator.h"
#include "validator/MutateValidator.h"
#include "validator/ACLValidator.h"

namespace nebula {
namespace graph {
std::unique_ptr<Validator> Validator::makeValidator(Sentence* sentence, QueryContext* context) {
    CHECK_NOTNULL(sentence);
    CHECK_NOTNULL(context);
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kSequential:
            return std::make_unique<SequentialValidator>(sentence, context);
        case Sentence::Kind::kGo:
            return std::make_unique<GoValidator>(sentence, context);
        case Sentence::Kind::kPipe:
            return std::make_unique<PipeValidator>(sentence, context);
        case Sentence::Kind::kAssignment:
            return std::make_unique<AssignmentValidator>(sentence, context);
        case Sentence::Kind::kSet:
            return std::make_unique<SetValidator>(sentence, context);
        case Sentence::Kind::kUse:
            return std::make_unique<UseValidator>(sentence, context);
        case Sentence::Kind::kGetSubgraph:
            return std::make_unique<GetSubgraphValidator>(sentence, context);
        case Sentence::Kind::kCreateSpace:
            return std::make_unique<CreateSpaceValidator>(sentence, context);
        case Sentence::Kind::kCreateTag:
            return std::make_unique<CreateTagValidator>(sentence, context);
        case Sentence::Kind::kCreateEdge:
            return std::make_unique<CreateEdgeValidator>(sentence, context);
        case Sentence::Kind::kDescribeSpace:
            return std::make_unique<DescSpaceValidator>(sentence, context);
        case Sentence::Kind::kDescribeTag:
            return std::make_unique<DescTagValidator>(sentence, context);
        case Sentence::Kind::kDescribeEdge:
            return std::make_unique<DescEdgeValidator>(sentence, context);
        case Sentence::Kind::kInsertVertices:
            return std::make_unique<InsertVerticesValidator>(sentence, context);
        case Sentence::Kind::kInsertEdges:
            return std::make_unique<InsertEdgesValidator>(sentence, context);
        case Sentence::Kind::kCreateUser:
            return std::make_unique<CreateUserValidator>(sentence, context);
        case Sentence::Kind::kDropUser:
            return std::make_unique<DropUserValidator>(sentence, context);
        case Sentence::Kind::kAlterUser:
            return std::make_unique<UpdateUserValidator>(sentence, context);
        case Sentence::Kind::kChangePassword:
            return std::make_unique<ChangePasswordValidator>(sentence, context);
        case Sentence::Kind::kGrant:
            return std::make_unique<GrantRoleValidator>(sentence, context);
        case Sentence::Kind::kRevoke:
            return std::make_unique<RevokeRoleValidator>(sentence, context);
        default:
            return std::make_unique<ReportError>(sentence, context);
    }
}

Status Validator::appendPlan(PlanNode* node, PlanNode* appended) {
    switch (node->kind()) {
        case PlanNode::Kind::kFilter:
        case PlanNode::Kind::kProject:
        case PlanNode::Kind::kSort:
        case PlanNode::Kind::kLimit:
        case PlanNode::Kind::kAggregate:
        case PlanNode::Kind::kSelect:
        case PlanNode::Kind::kLoop:
        case PlanNode::Kind::kCreateUser:
        case PlanNode::Kind::kDropUser:
        case PlanNode::Kind::kUpdateUser:
        case PlanNode::Kind::kGrantRole:
        case PlanNode::Kind::kRevokeRole:
        case PlanNode::Kind::kChangePassword:
        case PlanNode::Kind::kListUserRoles:
        case PlanNode::Kind::kListUsers:
        case PlanNode::Kind::kListRoles:
        case PlanNode::Kind::kSwitchSpace: {
            static_cast<SingleInputNode*>(node)->setInput(appended);
            break;
        }
        default: {
            return Status::Error("%s not support to append an input.",
                                 PlanNode::toString(node->kind()));
        }
    }
    return Status::OK();
}

Status Validator::validate() {
    Status status;

    if (!vctx_) {
        VLOG(1) << "Validate context was not given.";
        return Status::Error("Validate context was not given.");
    }

    if (!sentence_) {
        VLOG(1) << "Sentence was not given";
        return Status::Error("Sentence was not given");
    }

    if (!noSpaceRequired_ && !spaceChosen()) {
        VLOG(1) << "Space was not chosen.";
        status = Status::Error("Space was not chosen.");
        return status;
    }

    status = validateImpl();
    if (!status.ok()) {
        return status;
    }

    status = toPlan();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

bool Validator::spaceChosen() {
    return vctx_->spaceChosen();
}

std::vector<std::string> Validator::evalResultColNames(const YieldColumns* cols) const {
    std::vector<std::string> colNames;
    for (auto& col : cols->columns()) {
        if (col->alias() != nullptr) {
            colNames.emplace_back(*col->alias());
        } else {
            switch (col->expr()->kind()) {
                case Expression::Kind::kInputProperty: {
                    auto expr = static_cast<InputPropertyExpression*>(col->expr());
                    colNames.emplace_back(*expr->sym());
                    break;
                }
                default: {
                    colNames.emplace_back(col->expr()->toString());
                    break;
                }
            }
        }
    }

    return colNames;
}
}  // namespace graph
}  // namespace nebula
