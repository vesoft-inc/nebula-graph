/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/match/InnerJoinStrategy.h"
#include "common/expression/AttributeExpression.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
PlanNode* InnerJoinStrategy::connect(const PlanNode* left, const PlanNode* right) {
    return joinDataSet(left, right);
}

PlanNode* InnerJoinStrategy::joinDataSet(const PlanNode* left, const PlanNode* right) {
    auto& leftKey = left->colNamesRef().back();
    auto& rightKey = right->colNamesRef().front();
    auto buildExpr = getLastEdgeDstExprInLastPath(leftKey);
    auto probeExpr = getFirstVertexVidInFistPath(rightKey);
    auto join = DataJoin::make(qctx_,
                               const_cast<PlanNode*>(right),
                               {left->outputVar(), 0},
                               {right->outputVar(), 0},
                               {buildExpr},
                               {probeExpr});
    std::vector<std::string> colNames = left->colNames();
    const auto& rightColNames = right->colNamesRef();
    colNames.insert(colNames.end(), rightColNames.begin(), rightColNames.end());
    join->setColNames(std::move(colNames));
    return join;
}

Expression* InnerJoinStrategy::getLastEdgeDstExprInLastPath(const std::string& colName) {
    // expr: __Project_2[-1] => path
    auto columnExpr = ExpressionUtils::inputPropExpr(colName);
    // expr: endNode(path) => vn
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(columnExpr));
    auto fn = std::make_unique<std::string>("endNode");
    auto endNode = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    // expr: en[_dst] => dst vid
    auto vidExpr = std::make_unique<ConstantExpression>(kVid);
    return new AttributeExpression(endNode.release(), vidExpr.release());
}

Expression* InnerJoinStrategy::getFirstVertexVidInFistPath(const std::string& colName) {
    // expr: __Project_2[0] => path
    auto columnExpr = ExpressionUtils::inputPropExpr(colName);
    // expr: startNode(path) => v1
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(columnExpr));
    auto fn = std::make_unique<std::string>("startNode");
    auto firstVertexExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    // expr: v1[_vid] => vid
    return new AttributeExpression(firstVertexExpr.release(), new ConstantExpression(kVid));
}

}  // namespace graph
}  // namespace nebula
