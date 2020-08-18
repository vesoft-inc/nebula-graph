/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/Validator.h"

#include "parser/Sentence.h"
#include "planner/Query.h"
#include "util/SchemaUtil.h"
#include "util/ExpressionUtils.h"
#include "validator/AdminValidator.h"
#include "validator/AssignmentValidator.h"
#include "validator/ExplainValidator.h"
#include "validator/GetSubgraphValidator.h"
#include "validator/GoValidator.h"
#include "validator/LimitValidator.h"
#include "validator/MaintainValidator.h"
#include "validator/MutateValidator.h"
#include "validator/OrderByValidator.h"
#include "validator/PipeValidator.h"
#include "validator/FetchVerticesValidator.h"
#include "validator/FetchEdgesValidator.h"
#include "validator/ReportError.h"
#include "validator/SequentialValidator.h"
#include "validator/SetValidator.h"
#include "validator/UseValidator.h"
#include "validator/ACLValidator.h"
#include  "validator/BalanceValidator.h"
#include "validator/AdminJobValidator.h"
#include "validator/YieldValidator.h"
#include "validator/GroupByValidator.h"
#include "common/function/FunctionManager.h"

namespace nebula {
namespace graph {

Validator::Validator(Sentence* sentence, QueryContext* qctx)
    : sentence_(DCHECK_NOTNULL(sentence)),
      qctx_(DCHECK_NOTNULL(qctx)),
      vctx_(DCHECK_NOTNULL(qctx->vctx())) {}

std::unique_ptr<Validator> Validator::makeValidator(Sentence* sentence, QueryContext* context) {
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kExplain:
            return std::make_unique<ExplainValidator>(sentence, context);
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
        case Sentence::Kind::kLimit:
            return std::make_unique<LimitValidator>(sentence, context);
        case Sentence::Kind::kOrderBy:
            return std::make_unique<OrderByValidator>(sentence, context);
        case Sentence::Kind::kYield:
            return std::make_unique<YieldValidator>(sentence, context);
        case Sentence::Kind::kGroupBy:
            return std::make_unique<GroupByValidator>(sentence, context);
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
        case Sentence::Kind::kAlterTag:
            return std::make_unique<AlterTagValidator>(sentence, context);
        case Sentence::Kind::kAlterEdge:
            return std::make_unique<AlterEdgeValidator>(sentence, context);
        case Sentence::Kind::kShowSpaces:
            return std::make_unique<ShowSpacesValidator>(sentence, context);
        case Sentence::Kind::kShowTags:
            return std::make_unique<ShowTagsValidator>(sentence, context);
        case Sentence::Kind::kShowEdges:
            return std::make_unique<ShowEdgesValidator>(sentence, context);
        case Sentence::Kind::kDropSpace:
            return std::make_unique<DropSpaceValidator>(sentence, context);
        case Sentence::Kind::kDropTag:
            return std::make_unique<DropTagValidator>(sentence, context);
        case Sentence::Kind::kDropEdge:
            return std::make_unique<DropEdgeValidator>(sentence, context);
        case Sentence::Kind::kShowCreateSpace:
            return std::make_unique<ShowCreateSpaceValidator>(sentence, context);
        case Sentence::Kind::kShowCreateTag:
            return std::make_unique<ShowCreateTagValidator>(sentence, context);
        case Sentence::Kind::kShowCreateEdge:
            return std::make_unique<ShowCreateEdgeValidator>(sentence, context);
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
        case Sentence::Kind::kShowUsers:
            return std::make_unique<ShowUsersValidator>(sentence, context);
        case Sentence::Kind::kChangePassword:
            return std::make_unique<ChangePasswordValidator>(sentence, context);
        case Sentence::Kind::kGrant:
            return std::make_unique<GrantRoleValidator>(sentence, context);
        case Sentence::Kind::kRevoke:
            return std::make_unique<RevokeRoleValidator>(sentence, context);
        case Sentence::Kind::kShowRoles:
            return std::make_unique<ShowRolesInSpaceValidator>(sentence, context);
        case Sentence::Kind::kBalance:
            return std::make_unique<BalanceValidator>(sentence, context);
        case Sentence::Kind::kAdminJob:
            return std::make_unique<AdminJobValidator>(sentence, context);
        case Sentence::Kind::kFetchVertices:
            return std::make_unique<FetchVerticesValidator>(sentence, context);
        case Sentence::Kind::kFetchEdges:
            return std::make_unique<FetchEdgesValidator>(sentence, context);
        case Sentence::Kind::kCreateSnapshot:
            return std::make_unique<CreateSnapshotValidator>(sentence, context);
        case Sentence::Kind::kDropSnapshot:
            return std::make_unique<DropSnapshotValidator>(sentence, context);
        case Sentence::Kind::kShowSnapshots:
            return std::make_unique<ShowSnapshotsValidator>(sentence, context);
        case Sentence::Kind::kDeleteVertices:
            return std::make_unique<DeleteVerticesValidator>(sentence, context);
        case Sentence::Kind::kDeleteEdges:
            return std::make_unique<DeleteEdgesValidator>(sentence, context);
        case Sentence::Kind::kUpdateVertex:
            return std::make_unique<UpdateVertexValidator>(sentence, context);
        case Sentence::Kind::kUpdateEdge:
            return std::make_unique<UpdateEdgeValidator>(sentence, context);
        case Sentence::Kind::kShowHosts:
            return std::make_unique<ShowHostsValidator>(sentence, context);
        case Sentence::Kind::kShowParts:
            return std::make_unique<ShowPartsValidator>(sentence, context);
        case Sentence::Kind::kShowCharset:
            return std::make_unique<ShowCharsetValidator>(sentence, context);
        case Sentence::Kind::kShowCollation:
            return std::make_unique<ShowCollationValidator>(sentence, context);
        case Sentence::Kind::kGetConfig:
            return std::make_unique<GetConfigValidator>(sentence, context);
        case Sentence::Kind::kSetConfig:
            return std::make_unique<SetConfigValidator>(sentence, context);
        case Sentence::Kind::kShowConfigs:
            return std::make_unique<ShowConfigsValidator>(sentence, context);
        case Sentence::Kind::kUnknown:
        case Sentence::Kind::kMatch:
        case Sentence::Kind::kCreateTagIndex:
        case Sentence::Kind::kShowCreateTagIndex:
        case Sentence::Kind::kShowTagIndexStatus:
        case Sentence::Kind::kDescribeTagIndex:
        case Sentence::Kind::kShowTagIndexes:
        case Sentence::Kind::kRebuildTagIndex:
        case Sentence::Kind::kDropTagIndex:
        case Sentence::Kind::kCreateEdgeIndex:
        case Sentence::Kind::kShowCreateEdgeIndex:
        case Sentence::Kind::kShowEdgeIndexStatus:
        case Sentence::Kind::kDescribeEdgeIndex:
        case Sentence::Kind::kShowEdgeIndexes:
        case Sentence::Kind::kRebuildEdgeIndex:
        case Sentence::Kind::kDropEdgeIndex:
        case Sentence::Kind::kLookup:
        case Sentence::Kind::kDownload:
        case Sentence::Kind::kIngest:
        case Sentence::Kind::kFindPath:
        case Sentence::Kind::kReturn: {
            // nothing
            DLOG(FATAL) << "Unimplemented sentence " << kind;
        }
    }
    DLOG(FATAL) << "Unknown sentence " << static_cast<int>(kind);
    return std::make_unique<ReportError>(sentence, context);
}

Status Validator::appendPlan(PlanNode* node, PlanNode* appended) {
    switch (DCHECK_NOTNULL(node)->kind()) {
        case PlanNode::Kind::kShowHosts:
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
        case PlanNode::Kind::kMultiOutputs:
        case PlanNode::Kind::kSwitchSpace:
        case PlanNode::Kind::kGetEdges:
        case PlanNode::Kind::kGetVertices:
        case PlanNode::Kind::kCreateSpace:
        case PlanNode::Kind::kCreateTag:
        case PlanNode::Kind::kCreateEdge:
        case PlanNode::Kind::kDescSpace:
        case PlanNode::Kind::kDescTag:
        case PlanNode::Kind::kDescEdge:
        case PlanNode::Kind::kInsertVertices:
        case PlanNode::Kind::kInsertEdges:
        case PlanNode::Kind::kGetNeighbors:
        case PlanNode::Kind::kAlterTag:
        case PlanNode::Kind::kAlterEdge:
        case PlanNode::Kind::kShowCreateSpace:
        case PlanNode::Kind::kShowCreateTag:
        case PlanNode::Kind::kShowCreateEdge:
        case PlanNode::Kind::kDropSpace:
        case PlanNode::Kind::kDropTag:
        case PlanNode::Kind::kDropEdge:
        case PlanNode::Kind::kShowSpaces:
        case PlanNode::Kind::kShowTags:
        case PlanNode::Kind::kShowEdges:
        case PlanNode::Kind::kCreateSnapshot:
        case PlanNode::Kind::kDropSnapshot:
        case PlanNode::Kind::kSubmitJob:
        case PlanNode::Kind::kShowSnapshots:
        case PlanNode::Kind::kBalanceLeaders:
        case PlanNode::Kind::kBalance:
        case PlanNode::Kind::kStopBalance:
        case PlanNode::Kind::kShowBalance:
        case PlanNode::Kind::kDeleteVertices:
        case PlanNode::Kind::kDeleteEdges:
        case PlanNode::Kind::kUpdateVertex:
        case PlanNode::Kind::kUpdateEdge:
        case PlanNode::Kind::kShowParts:
        case PlanNode::Kind::kShowCharset:
        case PlanNode::Kind::kShowCollation:
        case PlanNode::Kind::kShowConfigs:
        case PlanNode::Kind::kSetConfig:
        case PlanNode::Kind::kGetConfig: {
            static_cast<SingleDependencyNode*>(node)->dependsOn(appended);
            break;
        }
        default: {
            return Status::SemanticError("%s not support to append an input.",
                                         PlanNode::toString(node->kind()));
        }
    }
    return Status::OK();
}

Status Validator::appendPlan(PlanNode* tail) {
    return appendPlan(tail_, DCHECK_NOTNULL(tail));
}

Status Validator::validate() {
    if (!vctx_) {
        VLOG(1) << "Validate context was not given.";
        return Status::SemanticError("Validate context was not given.");
    }

    if (!sentence_) {
        VLOG(1) << "Sentence was not given";
        return Status::SemanticError("Sentence was not given");
    }

    if (!noSpaceRequired_ && !spaceChosen()) {
        VLOG(1) << "Space was not chosen.";
        return Status::SemanticError("Space was not chosen.");
    }

    if (!noSpaceRequired_) {
        space_ = vctx_->whichSpace();
    }

    auto status = validateImpl();
    if (!status.ok()) {
        if (status.isSemanticError()) return status;
        return Status::SemanticError(status.message());
    }

    status = toPlan();
    if (!status.ok()) {
        if (status.isSemanticError()) return status;
        return Status::SemanticError(status.message());
    }

    return Status::OK();
}

bool Validator::spaceChosen() {
    return vctx_->spaceChosen();
}

std::vector<std::string> Validator::deduceColNames(const YieldColumns* cols) const {
    std::vector<std::string> colNames;
    for (auto col : cols->columns()) {
        colNames.emplace_back(deduceColName(col));
    }
    return colNames;
}

std::string Validator::deduceColName(const YieldColumn* col) const {
    if (col->alias() != nullptr) {
        return *col->alias();
    } else {
        return col->toString();
    }
}

// static
Status Validator::checkPropNonexistOrDuplicate(const ColsDef& cols,
                                               const folly::StringPiece& prop,
                                               const std::string& validatorName) {
    auto eq = [&](const ColDef& col) { return col.first == prop.str(); };
    auto iter = std::find_if(cols.cbegin(), cols.cend(), eq);
    if (iter == cols.cend()) {
        return Status::SemanticError("%s: prop `%s' not exists",
                                      validatorName.c_str(),
                                      prop.str().c_str());
    }

    iter = std::find_if(iter + 1, cols.cend(), eq);
    if (iter != cols.cend()) {
        return Status::SemanticError("%s: duplicate prop `%s'",
                                      validatorName.c_str(),
                                      prop.str().c_str());
    }

    return Status::OK();
}

StatusOr<std::string> Validator::checkRef(const Expression* ref, Value::Type type) const {
    if (ref->kind() == Expression::Kind::kInputProperty) {
        const auto* symExpr = static_cast<const SymbolPropertyExpression*>(ref);
        ColDef col(*symExpr->prop(), type);
        const auto find = std::find(inputs_.begin(), inputs_.end(), col);
        if (find == inputs_.end()) {
            return Status::Error("No input property %s", symExpr->prop()->c_str());
        }
        return std::string();
    } else if (ref->kind() == Expression::Kind::kVarProperty) {
        const auto* symExpr = static_cast<const SymbolPropertyExpression*>(ref);
        ColDef col(*symExpr->prop(), type);
        const auto &varName = *symExpr->sym();
        const auto &var = vctx_->getVar(varName);
        if (var.empty()) {
            return Status::Error("No variable %s", varName.c_str());
        }
        const auto find = std::find(var.begin(), var.end(), col);
        if (find == var.end()) {
            return Status::Error("No property %s in variable %s",
                                 symExpr->prop()->c_str(),
                                 varName.c_str());
        }
        return varName;
    } else {
        // it's guranteed by parser
        DLOG(FATAL) << "Unexpected expression " << ref->kind();
        return Status::Error("Unexpected expression.");
    }
}


}   // namespace graph
}   // namespace nebula

