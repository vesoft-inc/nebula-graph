/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"

#include "parser/TraverseSentences.h"
#include "planner/Query.h"
#include "expression/VariableExpression.h"
#include "expression/UnaryExpression.h"
#include "expression/ConstantExpression.h"

namespace nebula {
namespace graph {
Status GetSubgraphValidator::validateImpl() {
    Status status;
    auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);
    do {
        status = validateStep(gsSentence->step());
        if (!status.ok()) {
            break;
        }

        status = validateFrom(gsSentence->from());
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

    return Status::OK();
}

Status GetSubgraphValidator::validateStep(StepClause* step) {
    if (step == nullptr) {
        return Status::Error("Step clause was not declared.");
    }

    if (step->isUpto()) {
        return Status::Error("Get Subgraph not support upto.");
    }
    steps_ = step->steps();
    return Status::OK();
}

Status GetSubgraphValidator::validateFrom(FromClause* from) {
    if (from == nullptr) {
        return Status::Error("From clause was not declared.");
    }

    if (from->isRef()) {
        srcRef_ = from->ref();
    } else {
        for (auto* expr : from->vidList()) {
            // TODO:
            starts_.emplace_back(Expression::eval(expr));
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
    if (in != nullptr) {
        auto space = validateContext_->whichSpace();
        for (auto* e : in->edges()) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = validateContext_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            auto v = -et.value();
            edgeTypes_.emplace_back(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
    if (out != nullptr) {
        auto space = validateContext_->whichSpace();
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = validateContext_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            edgeTypes_.emplace_back(et.value());
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
    if (out != nullptr) {
        auto space = validateContext_->whichSpace();
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = validateContext_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            auto v = et.value();
            edgeTypes_.emplace_back(v);
            v = -v;
            edgeTypes_.emplace_back(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::toPlan() {
    auto* plan = validateContext_->plan();
    auto& space = validateContext_->whichSpace();

    // TODO:
    // loop -> project -> gn1 -> bodyStart
    auto* bodyStart = StartNode::make(plan);

    std::vector<EdgeType> edgeTypes;
    std::vector<storage::cpp2::PropExp> vertexProps;
    std::vector<storage::cpp2::PropExp> edgeProps;
    std::vector<storage::cpp2::StatProp> statProps;
    auto vidsToSave = validateContext_->varGen()->getVar();
    validateContext_->qctx()->setValue(vidsToSave, List(std::move(starts_)));
    auto* vids = new VariablePropertyExpression(
                new std::string(vidsToSave),
                new std::string("_vid"));
    auto* gn1 = GetNeighbors::make(
            plan,
            bodyStart,
            space.id,
            plan->saveObject(vids),
            std::move(edgeTypes),
            storage::cpp2::EdgeDirection::BOTH,  // FIXME: make direction right
            std::move(vertexProps),
            std::move(edgeProps),
            std::move(statProps));

    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(gn1->varName()),
                new std::string("_vid")),
            new std::string("_vid"));
    columns->addColumn(column);
    auto* project = Project::make(plan, gn1, plan->saveObject(columns));
    project->setVar(vidsToSave);

    // ++counter{0} <= steps
    auto counter = validateContext_->varGen()->getVar();
    validateContext_->qctx()->setValue(counter, 0);
    auto* condition = new RelationalExpression(
                Expression::Type::EXP_REL_LE,
                new UnaryExpression(
                        Expression::Type::EXP_UNARY_INCR,
                        new VersionedVariableExpression(
                                new std::string(counter),
                                new ConstantExpression(0))),
                new ConstantExpression(static_cast<int32_t>(steps_)));
    // The input of loop will set by father validator.
    auto* loop = Loop::make(plan, nullptr, project, plan->saveObject(condition));

    /*
    // selector -> loop
    // selector -> filter -> gn2 -> ifStrart
    auto* ifStart = StartNode::make(plan);

    std::vector<Row> starts;
    auto* gn2 = GetNeighbors::make(
            plan,
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
    columns = new YieldColumns();
    column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(gn2->varGenerated()),
                new std::string("_vid")),
            new std::string(listOfVids));
    column->setFunction(new std::string("collect"));
    columns->addColumn(column);
    auto* group = Aggregate::make(plan, gn2, plan->saveObject(columns));

    auto* filter = Filter::make(
                        plan,
                        group,
                        nullptr// TODO: build IN condition.
                        );
    auto* selector = Selector::make(plan, loop, filter, nullptr, nullptr);

    // TODO: A data collector.

    root_ = selector;
    tail_ = loop;
    */
    root_ = loop;
    tail_ = loop;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
