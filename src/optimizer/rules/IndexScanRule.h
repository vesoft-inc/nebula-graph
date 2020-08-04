/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_GRAPH_OPTIMIZER_INDEXSCANRULE_H_
#define NEBULA_GRAPH_OPTIMIZER_INDEXSCANRULE_H_

#include "optimizer/OptRule.h"
#include "planner/Query.h"
#include "optimizer/OptimizerUtils.h"

namespace nebula {
namespace graph {

using IndexOptItem = std::tuple<std::string, Expression::Kind, Value>;
using IndexOptItems = std::vector<IndexOptItem>;
using BoundaryValueType = OptimizerUtils::BoundValueType;

class IndexScanRule final : public OptRule {
public:
    bool match(const std::shared_ptr<PlanNode>& node) const override;

    StatusOr<std::shared_ptr<PlanNode>> transform(std::shared_ptr<PlanNode> node) override;

private:
    enum class ScanKind {
        UNKNOWN = 0,
        LOGICAL_OR,
        LOGICAL_AND,
    };

    IndexScanRule(QueryContext* qctx)
    : OptRule("IndexScanRule", qctx) {}

    Status prepareOptimize(const std::shared_ptr<PlanNode>& node);

    Status doOptimize();

    Status postOptimize();

    Status analyzeExpression(Expression* expr);

    Status analyzeIndexes();

    Status analyzeIndexesWithOR(std::vector<std::shared_ptr<meta::cpp2::IndexItem>>&& indexes);

    Status analyzeIndexesWithAND(std::vector<std::shared_ptr<meta::cpp2::IndexItem>>&& indexes);

    Status analyzeIndexesWithSimple(std::vector<std::shared_ptr<meta::cpp2::IndexItem>>&& indexes);

    StatusOr<meta::cpp2::IndexItem*>
    matchIndex(const IndexOptItems& items,
               const std::vector<std::shared_ptr<meta::cpp2::IndexItem>>& indexes);

    StatusOr<meta::cpp2::IndexItem*>
    findBestIndex(const IndexOptItems& items, const std::set<meta::cpp2::IndexItem*>& indexes);

    Status assembleIndexQueryCtx(const meta::cpp2::IndexItem* index, const IndexOptItems& items);

    Status assembleIndexColumnHint(const std::unordered_set<IndexOptItem>& items,
                                   const meta::cpp2::ColumnDef& col,
                                   IndexScan::IndexQueryCtx& ctx);

    StatusOr<std::pair<Value, Value>>
    rangScanPair(const meta::cpp2::ColumnDef& col, const IndexOptItem& item);

    std::unique_ptr<Expression>
    assembleIndexFilter(const std::string& schema, const IndexOptItems& items);

    Status writeRelationalExpr(RelationalExpression* expr);

    Expression::Kind reverseRelationalExprKind(Expression::Kind kind);
private:
    GraphSpaceID                      space_{-1};
    bool                              isEdgeIndex_{false};
    int32_t                           schemaId_{-1};
    std::unique_ptr<Expression>       rootFilter_{nullptr};
    IndexScan*                        node_{nullptr};
    ScanKind                          rootFilterkind_{ScanKind::UNKNOWN};
    IndexScan::IndexQueryCtxs         ctxs_{};
    IndexOptItems                     relationalOpList_{};
};

}   // namespace graph
}   // namespace nebula

#endif   // NEBULA_GRAPH_OPTIMIZER_INDEXSCANRULE_H_
