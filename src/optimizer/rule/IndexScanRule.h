/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef OPTIMIZER_INDEXSCANRULE_H_
#define OPTIMIZER_INDEXSCANRULE_H_

#include "optimizer/OptRule.h"
#include "planner/Query.h"
#include "optimizer/OptimizerUtils.h"

namespace nebula {
namespace graph {
class IndexScan;
}   // namespace graph
namespace opt {

using graph::IndexScan;
using graph::OptimizerUtils;
using graph::PlanNode;
using graph::QueryContext;
using storage::cpp2::IndexQueryContext;
using storage::cpp2::IndexColumnHint;
using BVO = graph::OptimizerUtils::BoundValueOperator;
using IndexItem = std::shared_ptr<meta::cpp2::IndexItem>;
using IndexQueryCtx = std::unique_ptr<std::vector<IndexQueryContext>>;

class IndexScanRule final : public OptRule {
    FRIEND_TEST(IndexScanRuleTest, BoundValueTest);
    FRIEND_TEST(IndexScanRuleTest, IQCtxTest);

public:
    static std::unique_ptr<OptRule> kInstance;

    bool match(const OptGroupExpr *groupExpr) const override;

    Status transform(graph::QueryContext *qctx,
                     const OptGroupExpr *groupExpr,
                     TransformResult *result) const override;

    std::string toString() const override;

private:
    struct ScanKind {
        enum class Kind {
            UNKNOWN = 0,
            LOGICAL_OR,
            LOGICAL_AND,
        };

    private:
        Kind kind_;

    public:
        ScanKind() {
            kind_ = Kind::UNKNOWN;
        }
        void setKind(Kind k) {
            kind_ = k;
        }
        Kind getKind() {
            return kind_;
        }
        bool isLogicalAnd() {
            return kind_ == Kind::LOGICAL_AND;
        }
    };

    // col_   : index column name
    // relOP_ : Relational operator , for example c1 > 1 , the relOP_ == kRelGT
    //                                            1 > c1 , the relOP_ == kRelLT
    // value_ : Constant value. from ConstantExpression.
    struct FilterItem {
        std::string                 col_;
        RelationalExpression::Kind  relOP_;
        Value                       value_;

        FilterItem(const std::string& col,
                   RelationalExpression::Kind relOP,
                   const Value& value)
                   : col_(col)
                   , relOP_(relOP)
                   , value_(value) {}
    };

    // FilterItems used for optimal index fetch and index scan context optimize.
    // for example : where c1 > 1 and c1 < 2 , the FilterItems should be :
    //               {c1, kRelGT, 1} , {c1, kRelLT, 2}
    struct FilterItems {
        std::vector<FilterItem> items;
        FilterItems() {}
        explicit FilterItems(const std::vector<FilterItem>& i) {
            items = i;
        }
        void addItem(const std::string& field,
                      RelationalExpression::Kind kind,
                      const Value& v) {
            items.emplace_back(FilterItem(field, kind, v));
        }
    };

    IndexScanRule();

    Status createIndexQueryCtx(IndexQueryCtx &iqctx,
                               ScanKind kind,
                               const FilterItems& items,
                               graph::QueryContext *qctx,
                               const OptGroupExpr *groupExpr) const;

    Status createIQCWithLogicAnd(IndexQueryCtx &iqctx,
                                 const FilterItems& items,
                                 graph::QueryContext *qctx,
                                 const OptGroupExpr *groupExpr) const;

    Status createIQCWithLogicOR(IndexQueryCtx &iqctx,
                                const FilterItems& items,
                                graph::QueryContext *qctx,
                                const OptGroupExpr *groupExpr) const;

    Status appendIQCtx(const IndexItem& index,
                       const FilterItems& items,
                       IndexQueryCtx &iqctx) const;

    Status appendColHint(std::vector<IndexColumnHint>& hitns,
                         const FilterItems& items,
                         const meta::cpp2::ColumnDef& col) const;

    Status boundValue(const FilterItem& item,
                      const meta::cpp2::ColumnDef& col,
                      Value& begin, Value& end) const;

    bool isEdge(const OptGroupExpr *groupExpr) const;

    int32_t schemaId(const OptGroupExpr *groupExpr) const;

    GraphSpaceID spaceId(const OptGroupExpr *groupExpr) const;

    std::unique_ptr<Expression> filterExpr(const OptGroupExpr *groupExpr) const;

    Status analyzeExpression(Expression* expr, FilterItems* items,
                             ScanKind* kind, bool isEdge) const;

    template <typename E,
              typename = std::enable_if_t<std::is_same<E, EdgePropertyExpression>::value ||
                                          std::is_same<E, TagPropertyExpression>::value>>
    Status addFilterItem(RelationalExpression* expr, FilterItems* items) const;

    Expression::Kind reverseRelationalExprKind(Expression::Kind kind) const;

    IndexItem findOptimalIndex(graph::QueryContext *qctx,
                               const OptGroupExpr *groupExpr,
                               const FilterItems& items) const;

    std::vector<IndexItem>
    allIndexesBySchema(graph::QueryContext *qctx, const OptGroupExpr *groupExpr) const;

    std::vector<IndexItem> findValidIndex(graph::QueryContext *qctx,
                                          const OptGroupExpr *groupExpr,
                                          const FilterItems& items) const;

    std::vector<IndexItem> findIndexForEqualScan(const std::vector<IndexItem>& indexes,
                                                 const FilterItems& items) const;

    std::vector<IndexItem> findIndexForRangeScan(const std::vector<IndexItem>& indexes,
                                                 const FilterItems& items) const;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_INDEXSCANRULE_H_
