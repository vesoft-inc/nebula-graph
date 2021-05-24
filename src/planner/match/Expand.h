/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_EXPAND_H_
#define PLANNER_MATCH_EXPAND_H_

#include "common/base/Base.h"
#include "context/ast/CypherAstContext.h"
#include "planner/plan/PlanNode.h"
#include "planner/Planner.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
/*
 * The Expand was designed to handle the pattern expanding.
 */
class Expand final {
public:
    Expand(MatchClauseContext* matchCtx, std::unique_ptr<Expression> initialExpr)
        : matchCtx_(matchCtx), initialExpr_(std::move(initialExpr)) {}

    Expand* reversely() {
        reversely_ = true;
        return this;
    }

    Expand* depends(PlanNode* dep) {
        dependency_ = dep;
        return this;
    }

    Expand* inputVar(const std::string& inputVar) {
        inputVar_ = inputVar;
        return this;
    }

    Status doExpand(const NodeInfo& node,
                    const EdgeInfo& edge,
                    const NodeInfo& dstNode,
                    SubPlan* plan);

private:
    Status expandSteps(const NodeInfo& node,
                       const EdgeInfo& edge,
                       const NodeInfo& dstNode,
                       SubPlan* plan);

    Status expand(const EdgeInfo& edge,
                  const NodeInfo& dstNode,
                  PlanNode* dep,
                  const std::string& inputVar,
                  const Expression* nodeFilter,
                  SubPlan* plan);

    Status expandStep(const EdgeInfo& edge,
                      PlanNode* dep,
                      const std::string& inputVar,
                      const Expression* nodeFilter,
                      SubPlan* plan,
                      const NodeInfo& dstNode,
                      bool withDst = false);

    Status collectData(const PlanNode* joinLeft,
                       const PlanNode* joinRight,
                       PlanNode** passThrough,
                       SubPlan* plan);

    Status filterDatasetByPathLength(const EdgeInfo& edge,
                                     PlanNode* input,
                                     SubPlan* plan);

    Expression* buildNStepLoopCondition(int64_t startIndex, int64_t maxHop) const;

    Expression* buildExpandEndCondition(const std::string &lastStepResult) const;

    Expression* buildExpandCondition(const std::string &lastStepResult,
                                     int64_t startIndex,
                                     int64_t maxHop) {
        return ExpressionUtils::And(buildNStepLoopCondition(startIndex, maxHop),
                                    buildExpandEndCondition(lastStepResult));
    }

    template <typename T>
    T* saveObject(T* obj) const {
        return matchCtx_->qctx->objPool()->add(obj);
    }

    std::unique_ptr<std::vector<storage::cpp2::EdgeProp>> genEdgeProps(const EdgeInfo &edge);

    void extractAndDedupVidDstColumns(QueryContext* qctx,
                                      Expression* initialExpr,
                                      PlanNode* dep,
                                      const std::string& inputVar,
                                      SubPlan& plan,
                                      const std::string &dstNodeAlias);

    Expression* initialExprOrExpandDstExpr(Expression* initialExpr,
                                           const std::string& inputVar,
                                           const std::string &dstNodeAlias);

    Expression* expandDstExpr(const std::string &inputVar, const std::string &dstNodeAlias) {
        auto find = reversely_ ?
                    matchCtx_->leftExpandFilledNodeId.find(dstNodeAlias) :
                    matchCtx_->rightExpandFilledNodeId.find(dstNodeAlias);
        DCHECK(find != matchCtx_->leftExpandFilledNodeId.end());
        DCHECK(find != matchCtx_->rightExpandFilledNodeId.end());
        CHECK_EQ(inputVar, find->second.first->outputVar());
        return find->second.second->clone().release();
    }

    bool expandInto(const std::string &dstNodeAlias) {
        if (reversely_) {
            return matchCtx_->leftExpandFilledNodeId.find(dstNodeAlias) !=
                   matchCtx_->leftExpandFilledNodeId.end();
        } else {
            return matchCtx_->rightExpandFilledNodeId.find(dstNodeAlias) !=
                   matchCtx_->rightExpandFilledNodeId.end();
        }
    }

    MatchClauseContext*                 matchCtx_;
    std::unique_ptr<Expression>         initialExpr_;
    bool                                reversely_{false};
    PlanNode*                           dependency_{nullptr};
    std::string                         inputVar_;
};
}   // namespace graph
}   // namespace nebula
#endif  // PLANNER_MATCH_EXPAND_H_
