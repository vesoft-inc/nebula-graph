/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GroupByValidator.h"
#include "planner/Query.h"


namespace nebula {
namespace graph {

Status GroupByValidator::validateImpl() {
    auto *groupBySentence = static_cast<GroupBySentence*>(sentence_);
    NG_RETURN_IF_ERROR(validateGroup(groupBySentence->groupClause()));
    NG_RETURN_IF_ERROR(validateYield(groupBySentence->yieldClause()));
    NG_RETURN_IF_ERROR(validateAll());
    return Status::OK();
}

Status GroupByValidator::validateGroup(const GroupClause *groupClause) {
    std::vector<YieldColumn*> columns;
    if (groupClause != nullptr) {
        columns = groupClause->columns();
    }

    if (columns.empty()) {
        return Status::SyntaxError("Group cols is Empty");
    }
    for (auto* col : columns) {
        if (!col->getAggFunName().empty()) {
            return Status::SyntaxError("Use invalid group function `%s`",
                                       col->getAggFunName().c_str());
        }

        auto status = deduceExprType(col->exr());
        NG_RETURN_IF_ERROR(status);

        NG_RETURN_IF_ERROR(deduceProps(col->expr()));
        groupCols_.emplace_back(col);
    }
    return Status::OK();
}


Status GroupByValidator::validateYield(const YieldClause *yieldClause) {
    std::vector<YieldColumn*> columns;
    if (yieldClause != nullptr) {
        columns = yieldClause->columns();
    }
    if (columns.empty()) {
        return Status::SyntaxError("Yield cols is Empty");
    }

    for (auto* col : columns) {
        auto fun = col->getAggFunName();
        if (!fun.empty()) {
            auto iter = AggFun::nameIdMap_.find(fun);
            if (iter == AggFun::nameIdMap_.end()) {
                return Status::Error("Unkown aggregate function `%s`", fun.c_str());
            }
            if (iter->second != AggFun::Function::kCount && col->expr()->toString() == "*") {
                return Status::SemanticError("`%s` invaild, * valid in count.",
                                             col->toString().c_str());
            }
        }

        auto status = deduceExprType(col->exr());
        NG_RETURN_IF_ERROR(status);
        auto type = std::move(status).value();
        auto name = deduceColName(col);
        outputs_.emplace_back(name, type);

        if (std::find(inputProps_.begin(), inputProps_.end(), "f") == inputProps_.end() && true) {
            return Status::OK();
        }

        // NG_RETURN_IF_ERROR(deduceProps(col->expr()));
        yieldCols_.emplace_back(col);

        if (col->alias() != nullptr) {
                aliases_.emplace(*col->alias(), col);
        }
    }
    return Status::OK();
}

Status GroupByValidator::validateAll() {
    // check group col
    std::unordered_set<std::string> inputGroupCols;
    for (auto& it : groupCols_) {
        // check input col
        if (it->expr()->kind() == Expression::Kind::kInputProperty) {
            auto groupName = static_cast<InputPropertyExpression*>(it->expr())->prop();
            inputGroupCols.emplace(*groupName);
            continue;
        }


        // check alias col
        auto groupName = it->expr()->toString();
        auto alisaIter = aliases_.find(groupName);
        if (alisaIter != aliases_.end()) {
            it = alisaIter->second;
            auto gName = static_cast<InputPropertyExpression*>(it->expr())->prop();
            if (it->expr()->kind() == Expression::Kind::kInputProperty) {
                inputGroupCols.emplace(*gName);
            }
            continue;
        }
        return Status::SyntaxError("Group `%s` isn't in output fields", groupName.c_str());
    }

    // check yield cols
    for (auto& it : yieldCols_) {
        if (it->expr()->kind() == Expression::Kind::kInputProperty) {
            auto yieldName = static_cast<InputPropertyExpression*>(it->expr())->prop();


            // check input yield filed without agg function and not in group cols
            if (inputGroupCols.find(*yieldName) == inputGroupCols.end() &&
                it->getAggFunName().empty()) {
                LOG(ERROR) << "Yield `" << *yieldName << "` isn't in group fields";
                return Status::SyntaxError("Yield `%s` isn't in group fields", yieldName->c_str());
            }
        } else if (it->expr()->kind() == Expression::Kind::kVarProperty) {
            LOG(ERROR) << "Can't support variableExpression: " << it->expr()->toString();
            return Status::SyntaxError("Can't support variableExpression");
        }
    }
    return Status::OK();
}


Status GroupByValidator::toPlan() {
    auto *groupBySentence = static_cast<GroupBySentence *>(sentence_);

    auto *plan = qctx_->plan();
    PlanNode *groupBy = GroupBy::make(plan, plan->root(),
                                      groupBySentence->groupClause()->columns());

    if (!yieldCols_.empty()) {
        PlanNode *project =
            Project::make(plan, groupBy, groupBySentence->yieldClause()->yields());
        root_ = project;
    } else {
        root_ = groupBy;
    }
    tail_ = groupBy;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
