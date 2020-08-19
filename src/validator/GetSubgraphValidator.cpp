/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"

#include "common/expression/UnaryExpression.h"
#include "common/expression/Expression.h"
#include "util/ContainerConv.h"
#include "common/expression/VariableExpression.h"
#include "context/QueryExpressionContext.h"
#include "parser/TraverseSentences.h"
#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status GetSubgraphValidator::validateImpl() {
    Status status;
    auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);

    status = validateStep(gsSentence->step(), steps_);
    if (!status.ok()) {
        return status;
    }

    status = validateStarts(gsSentence->from(), from_);
    if (!status.ok()) {
        return status;
    }

    status = validateInBound(gsSentence->in());
    if (!status.ok()) {
        return status;
    }

    status = validateOutBound(gsSentence->out());
    if (!status.ok()) {
        return status;
    }

    status = validateBothInOutBound(gsSentence->both());
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
    if (in != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = in->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : edges) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            auto v = -et.value();
            edgeTypes_.emplace(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            edgeTypes_.emplace(et.value());
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            auto v = et.value();
            edgeTypes_.emplace(v);
            v = -v;
            edgeTypes_.emplace(v);
        }
    }

    return Status::OK();
}

/*
 * 1 steps   history: collectVid{0}
 * 2 steps   history: collectVid{-1} collectVid{0}
 * 3 steps   history: collectVid{-2} collectVid{-1} collectVid{0}
 * ...
 * n steps   history: collectVid{-n+1} ...  collectVid{-1} collectVid{0}
 */
Expression* GetSubgraphValidator::buildFilterCondition(int64_t step) {
    // where *._dst IN startVids OR *._dst IN collectVid{0}[0][0] OR *._dst IN collectVid{1}[0][0]
    // OR ... *._dst IN collectVid{step}[0][0]
    auto* dst = new EdgeDstIdExpression(new std::string("*"));
    if (step == 1) {
        auto* lastestVidsDataSet =
            new VersionedVariableExpression(&collectVar_, new ConstantExpression(0));
        auto* lastestVidsList = new SubscriptExpression(
            new SubscriptExpression(std::move(lastestVidsDataSet), new ConstantExpression(0)),
            new ConstantExpression(0));

        auto* left = new RelationalExpression(
            Expression::Kind::kRelIn, dst, new ListExpression(startVidList_));
        auto* right = new RelationalExpression(Expression::Kind::kRelIn, dst, lastestVidsList);
        return new LogicalExpression(Expression::Kind::kLogicalOr, left, right);
    }
    auto* historyVidsDataSet =
        new VersionedVariableExpression(&collectVar_, new ConstantExpression(1 - step));
    auto* historyVidsList = new SubscriptExpression(
        new SubscriptExpression(std::move(historyVidsDataSet), new ConstantExpression(0)),
        new ConstantExpression(0));
    auto* left = new RelationalExpression(Expression::Kind::kRelIn, dst, historyVidsList);
    auto* right = buildFilterCondition(step - 1);
    auto* result = new LogicalExpression(Expression::Kind::kLogicalOr, left, right);
    return result;
}

Status GetSubgraphValidator::toPlan() {
    auto& space = vctx_->whichSpace();
    // gn <- filter <- DataCollect
    //  |
    // loop(step) -> Agg(collect) -> project -> gn -> bodyStart
    auto* bodyStart = StartNode::make(plan);

    std::string startVidsVar;
    PlanNode* projectStartVid = nullptr;
    if (!from_.vids.empty() && from_.srcRef == nullptr) {
        startVidsVar = buildConstantInput();
    } else {
        projectStartVid = buildRuntimeInput();
        startVidsVar = projectStartVid->varName();
    }

    auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    auto statProps = std::make_unique<std::vector<storage::cpp2::StatProp>>();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    auto* gn =
        GetNeighbors::make(plan,
                           bodyStart,
                           space.id,
                           src_,
                           ContainerConv::to<std::vector>(std::move(edgeTypes_)),
                           // TODO(jmq) add syntax like `BOTH *`, `OUT *` ...
                           storage::cpp2::EdgeDirection::OUT_EDGE,   // FIXME: make direction right
                           std::move(vertexProps),
                           std::move(edgeProps),
                           std::move(statProps),
                           std::move(exprs),
                           true /*subgraph not need duplicate*/);
    gn->setInputVar(startVidsVar);

    auto* projectVids = projectDstVidsFromGN(gn, startVidsVar);

    auto var = vctx_->anonVarGen()->getVar();
    auto* column = new YieldColumn(
        new VariablePropertyExpression(new std::string(var), new std::string(kVid)),
        new std::string(kVid));
    column->setAggFunction(new std::string("COLLECT"));
    auto fun = column->getAggFunName();
    auto* collect =
        Aggregate::make(plan,
                        projectVids,
                        {},
                        {Aggregate::GroupItem(column->expr(), AggFun::nameIdMap_[fun], true)});
    collect->setInputVar(projectVids->varName());
    collect->setColNames({kVid});
    collectVar_ = collect->varName();

    // TODO(jmq) add condition when gn get empty result
    auto* condition = buildNStepLoopCondition(steps_.steps);
    auto* loop = Loop::make(plan, projectStartVid, collect, condition);

    vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    statProps = std::make_unique<std::vector<storage::cpp2::StatProp>>();
    exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    auto* gn1 =
        GetNeighbors::make(plan,
                           loop,
                           space.id,
                           src_,
                           ContainerConv::to<std::vector>(std::move(edgeTypes_)),
                           // TODO(jmq) add syntax like `BOTH *`, `OUT *` ...
                           storage::cpp2::EdgeDirection::OUT_EDGE,   // FIXME: make direction right
                           std::move(vertexProps),
                           std::move(edgeProps),
                           std::move(statProps),
                           std::move(exprs),
                           true /*subgraph not need duplicate*/);
    gn1->setInputVar(projectVids->varName());

    auto* filter = Filter::make(plan, gn1, buildFilterCondition(steps_.steps));
    filter->setInputVar(gn1->varName());
    filter->setColNames({kVid});

    // datacollect
    std::vector<std::string> collects = {gn->varName(), gn1->varName()};
    auto* dc =
        DataCollect::make(plan, filter, DataCollect::CollectKind::kSubgraph, std::move(collects));
    dc->setInputVar(filter->varName());
    dc->setColNames({"_vertices", "_edges"});
    root_ = dc;
    tail_ = loop;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
