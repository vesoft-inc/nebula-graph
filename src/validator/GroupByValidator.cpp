/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GroupByValidator.h"


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
    std::vector<YieldColumn*> groups;
    if (groupClause != nullptr) {
        groups = groupClause->columns();
    }

    if (groups.empty()) {
        return Status::SyntaxError("Group cols is Empty");
    }
    for (auto *col : groups) {
        if (col->getAggFunName() != "") {
            return Status::SyntaxError("Use invalid group function `%s`",
                                        col->getAggFunName().c_str());
        }
        NG_RETURN_IF_ERROR(deduceProps(col->expr()));
        groupCols_.emplace_back(col);
    }
    return Status::OK();
}

constexpr char kCount[] = "COUNT";
constexpr char kCountDist[] = "COUNT_DISTINCT";

Status GroupByValidator::validateYield(const YieldClause *yieldClause) {
    std::vector<YieldColumn*> yields;
    if (yieldClause != nullptr) {
        yields = yieldClause->columns();
    }
    if (yields.empty()) {
        return Status::SyntaxError("Yield cols is Empty");
    }
    for (auto* col : yields) {
        if ((col->getAggFunName() != kCount && col->getAggFunName() != kCountDist) &&
            col->expr()->toString() == "*") {
            return Status::SyntaxError("Syntax error: near `*`");
        }

        NG_RETURN_IF_ERROR(deduceProps(col->expr()));
        yieldCols_.emplace_back(col);

        if (col->alias() != nullptr) {
            if (col->expr()->kind() == Expression::Kind::kInputProperty) {
                aliases_.emplace(*col->alias(), col);
            }
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
            auto findIter = std::find_if(inputs_.begin(), inputs_.end(),
                                         [&groupName](const auto &pair) {
                                           return *groupName == pair.first;
                                         });

            if (findIter == inputs_.end()) {
                LOG(ERROR) << "Group `" << *groupName << "` isn't in output fields";
                return Status::SyntaxError("Group `%s` isn't in output fields", groupName->c_str());
            }
            inputGroupCols.emplace(*groupName);
            continue;
        }

        // Function call
        if (it->expr()->kind() == Expression::Kind::kFunctionCall) {
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
            auto findIter = std::find_if(inputs_.begin(), inputs_.end(),
                                         [&yieldName](const auto &pair) {
                                           return *yieldName == pair.first;
                                         });

            if (findIter == inputs_.end()) {
                LOG(ERROR) << "Yield `" << *yieldName << "` isn't in output fields";
                return Status::SyntaxError("Yield `%s` isn't in output fields", yieldName->c_str());
            }

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
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
