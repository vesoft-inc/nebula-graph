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
    return Status::OK();
}

Status GroupByValidator::validateGroup(GroupClause *groupClause) {
    std::vector<YieldColumn*> groups;
    if (groupClause != nullptr) {
        groups = groupClause->columns();
    }

    if (groups.empty()) {
        return Status::SyntaxError("Group cols is Empty");
    }
    for (auto *col : groups) {
        if (col->getAggFunName() != "") {
            return Status::SyntaxError("Use invalid group function `%s'",
                                        col->getAggFunName().c_str());
        }
        NG_RETURN_IF_ERROR(deduceProps(col->expr()));
        groupCols_.emplace_back(col);
    }
    return Status::OK();
}

Status GroupByValidator::validateYield(YieldClause *yieldClause) {
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


Status GroupByValidator::toPlan() {
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
