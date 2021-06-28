/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ngql/LookupPlanner.h"

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/expression/Expression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/PropertyExpression.h"
#include "context/ast/QueryAstContext.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "planner/Planner.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

static constexpr char kSrcVID[] = "SrcVID";
static constexpr char kDstVID[] = "DstVID";
static constexpr char kRanking[] = "Ranking";
static constexpr char kVertexID[] = "VertexID";

static const char* kEdgeKeys[3] = {kSrcVID, kDstVID, kRanking};

std::unique_ptr<Planner> LookupPlanner::make() {
    return std::unique_ptr<LookupPlanner>(new LookupPlanner());
}

bool LookupPlanner::match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kLookup;
}

StatusOr<SubPlan> LookupPlanner::transform(AstContext* astCtx) {
    auto lookupCtx = static_cast<LookupContext*>(astCtx);
    auto yieldCols = prepareReturnCols(lookupCtx);
    auto qctx = lookupCtx->qctx;
    auto from = static_cast<const LookupSentence*>(lookupCtx->sentence)->from();
    SubPlan plan;
    if (lookupCtx->isEdge) {
        plan.tail = EdgeIndexFullScan::make(qctx,
                                            nullptr,
                                            from,
                                            lookupCtx->space.id,
                                            {},
                                            returnCols_,
                                            lookupCtx->schemaId,
                                            lookupCtx->isEmptyResultSet);
    } else {
        plan.tail = TagIndexFullScan::make(qctx,
                                           nullptr,
                                           from,
                                           lookupCtx->space.id,
                                           {},
                                           returnCols_,
                                           lookupCtx->schemaId,
                                           lookupCtx->isEmptyResultSet);
    }

    plan.root = plan.tail;

    if (lookupCtx->filter) {
        plan.root = Filter::make(qctx, plan.root, lookupCtx->filter);
    }

    plan.root = Project::make(qctx, plan.root, yieldCols);
    return plan;
}

YieldColumns* LookupPlanner::prepareReturnCols(LookupContext* lookupCtx) {
    auto pool = lookupCtx->qctx->objPool();
    auto columns = pool->makeAndAdd<YieldColumns>();
    auto addColumn = [this, columns](const std::string& name) {
        auto expr = new InputPropertyExpression(name);
        columns->addColumn(new YieldColumn(expr, name));
        returnCols_.emplace_back(name);
    };
    if (lookupCtx->isEdge) {
        for (auto& key : kEdgeKeys) {
            addColumn(key);
        }
    } else {
        addColumn(kVertexID);
    }
    if (lookupCtx->withProject) {
        appendColumns(lookupCtx, columns);
    }
    return columns;
}

void LookupPlanner::appendColumns(LookupContext* lookupCtx, YieldColumns* columns) {
    auto sentence = static_cast<LookupSentence*>(lookupCtx->sentence);
    auto yieldClause = sentence->yieldClause();
    for (auto col : yieldClause->columns()) {
        auto expr = col->expr();
        DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
        auto laExpr = static_cast<LabelAttributeExpression*>(expr);
        const auto& schemaName = laExpr->left()->name();
        const auto& colName = laExpr->right()->value().getStr();
        Expression* propExpr = nullptr;
        if (lookupCtx->isEdge) {
            propExpr = new EdgePropertyExpression(schemaName, colName);
        } else {
            propExpr = new TagPropertyExpression(schemaName, colName);
        }
        columns->addColumn(new YieldColumn(propExpr, col->alias()));
        returnCols_.emplace_back(colName);
    }
}

}   // namespace graph
}   // namespace nebula
