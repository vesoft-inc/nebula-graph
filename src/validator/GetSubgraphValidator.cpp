/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"

#include "common/expression/UnaryExpression.h"

#include "util/ContainerConv.h"
#include "common/expression/VariableExpression.h"
#include "context/QueryExpressionContext.h"
#include "parser/TraverseSentences.h"
#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
GraphStatus GetSubgraphValidator::validateImpl() {
    GraphStatus status;
    auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);
    do {
        status = validateStep(gsSentence->step(), steps_);
        if (!status.ok()) {
            break;
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
    } while (false);

    return GraphStatus::OK();
}

GraphStatus GetSubgraphValidator::validateInBound(InBoundClause* in) {
    if (in != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = in->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : edges) {
            if (e->alias() != nullptr) {
                return GraphStatus::setSemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return GraphStatus::setEdgeNotFound(*e->edge());
            }

            auto v = -et.value();
            edgeTypes_.emplace(v);
        }
    }

    return GraphStatus::OK();
}

GraphStatus GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return GraphStatus::setSemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return GraphStatus::setEdgeNotFound(*e->edge());
            }

            edgeTypes_.emplace(et.value());
        }
    }

    return GraphStatus::OK();
}

GraphStatus GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return GraphStatus::setSemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return GraphStatus::setEdgeNotFound(*e->edge());
            }

            auto v = et.value();
            edgeTypes_.emplace(v);
            v = -v;
            edgeTypes_.emplace(v);
        }
    }

    return GraphStatus::OK();
}

GraphStatus GetSubgraphValidator::toPlan() {
    auto& space = vctx_->whichSpace();

    //                           bodyStart->gn->project(dst)
    //                                            |
    // start [->previous] [-> project(input)] -> loop -> collect
    auto* bodyStart = StartNode::make(qctx_);

    std::vector<EdgeType> edgeTypes;
    auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    auto statProps = std::make_unique<std::vector<storage::cpp2::StatProp>>();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();

    std::string startVidsVar;
    PlanNode* projectStartVid = nullptr;
    if (!from_.vids.empty() && from_.srcRef == nullptr) {
        startVidsVar = buildConstantInput();
    } else {
        projectStartVid = buildRuntimeInput();
        startVidsVar = projectStartVid->varName();
    }

    ResultBuilder builder;
    builder.value(Value(std::move(ds))).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(vidsToSave, builder.finish());
    auto* vids = new VariablePropertyExpression(
                     new std::string(vidsToSave),
                     new std::string(kVid));
    auto* gn1 = GetNeighbors::make(
            qctx_,
            bodyStart,
            space.id,
            plan->saveObject(vids),
            ContainerConv::to<std::vector>(std::move(edgeTypes_)),
            storage::cpp2::EdgeDirection::BOTH,  // FIXME: make direction right
            std::move(vertexProps),
            std::move(edgeProps),
            std::move(statProps),
            std::move(exprs),
            true /*subgraph not need duplicate*/);
    gn1->setInputVar(vidsToSave);

    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
        new EdgePropertyExpression(new std::string("*"), new std::string(kDst)),
        new std::string(kVid));
    columns->addColumn(column);
    auto* project = Project::make(plan, gn1, plan->saveObject(columns));
    project->setInputVar(gn1->varName());
    project->setOutputVar(vidsToSave);
    project->setColNames(deduceColNames(columns));

    // ++counter{0} <= steps
    // TODO(shylock) add condition when gn get empty result
    auto* condition = buildNStepLoopCondition(steps_.steps);
    // The input of loop will set by father validator.
    auto* loop = Loop::make(qctx_, nullptr, projectVids, condition);

    /*
    // selector -> loop
    // selector -> filter -> gn2 -> ifStrart
    auto* ifStart = StartNode::make(qctx_);

    std::vector<Row> starts;
    auto* gn2 = GetNeighbors::make(
            qctx_,
            ifStart,
            space.id,
            std::move(starts),
            vids1,
            std::move(edgeTypes),
            storage::cpp2::EdgeDirection::BOTH,  // FIXME: make edge direction right
            std::move(vertexProps),
            std::move(edgeProps),
            std::move(statProps));

    // collect(gn2._vids) as listofvids
    auto& listOfVids = varGen_->getVar();
    columns = qctx_->objPool()->add(new YieldColumns());
    column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(gn2->varGenerated()),
                new std::string(kVid)),
            new std::string(listOfVids));
    column->setFunction(new std::string("collect"));
    columns->addColumn(column);
    auto* group = Aggregate::make(qctx_, gn2, columns);

    auto* filter = Filter::make(
                        qctx_,
                        group,
                        nullptr// TODO: build IN condition.
                        );
    auto* selector = Selector::make(qctx_, loop, filter, nullptr, nullptr);

    // TODO: A data collector.

    root_ = selector;
    tail_ = loop;
    */
    std::vector<std::string> collects = {gn1->varName()};
    auto* dc = DataCollect::make(qctx_, loop,
            DataCollect::CollectKind::kSubgraph, std::move(collects));
    dc->setColNames({"_vertices", "_edges"});
    root_ = dc;
    tail_ = loop;
    return GraphStatus::OK();
}
}  // namespace graph
}  // namespace nebula
