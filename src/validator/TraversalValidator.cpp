/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/TraversalValidator.h"
#include "common/expression/VariableExpression.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

Status TraversalValidator::validateOver(const OverClause* clause, Over& over) {
    if (clause == nullptr) {
        return Status::Error("Over clause nullptr.");
    }

    over.direction = clause->direction();
    auto* schemaMng = qctx_->schemaMng();
    if (clause->isOverAll()) {
        auto allEdgeStatus = schemaMng->getAllEdge(space_.id);
        NG_RETURN_IF_ERROR(allEdgeStatus);
        auto edges = std::move(allEdgeStatus).value();
        if (edges.empty()) {
            return Status::Error("No edge type found in space %s",
                    space_.name.c_str());
        }
        for (auto edge : edges) {
            auto edgeType = schemaMng->toEdgeType(space_.id, edge);
            if (!edgeType.ok()) {
                return Status::Error("%s not found in space [%s].",
                        edge.c_str(), space_.name.c_str());
            }
            over.edgeTypes.emplace_back(edgeType.value());
        }
        over.allEdges = std::move(edges);
        over.isOverAll = true;
    } else {
        auto edges = clause->edges();
        for (auto* edge : edges) {
            auto edgeName = *edge->edge();
            auto edgeType = schemaMng->toEdgeType(space_.id, edgeName);
            if (!edgeType.ok()) {
                return Status::Error("%s not found in space [%s].",
                        edgeName.c_str(), space_.name.c_str());
            }
            over.edgeTypes.emplace_back(edgeType.value());
        }
    }
    return Status::OK();
}

Status TraversalValidator::validateStep(const StepClause* clause, Steps& step) {
    if (clause == nullptr) {
        return Status::Error("Step clause nullptr.");
    }
    if (clause->isMToN()) {
        auto* mToN = qctx_->objPool()->makeAndAdd<StepClause::MToN>();
        mToN->mSteps = clause->mToN()->mSteps;
        mToN->nSteps = clause->mToN()->nSteps;

        if (mToN->mSteps == 0 && mToN->nSteps == 0) {
            step.steps = 0;
            return Status::OK();
        }
        if (mToN->mSteps == 0) {
            mToN->mSteps = 1;
        }
        if (mToN->nSteps < mToN->mSteps) {
            return Status::Error("`%s', upper bound steps should be greater than lower bound.",
                                 clause->toString().c_str());
        }
        if (mToN->mSteps == mToN->nSteps) {
            steps_.steps = mToN->mSteps;
            return Status::OK();
        }
        step.mToN = mToN;
    } else {
        auto steps = clause->steps();
        step.steps = steps;
    }
    return Status::OK();
}



PlanNode* TraversalValidator::projectDstVidsFromGN(PlanNode* gn, const std::string& outputVar) {
    Project* project = nullptr;
    auto* columns = qctx_->objPool()->add(new YieldColumns());
    auto* column = new YieldColumn(
        new EdgePropertyExpression(new std::string("*"), new std::string(kDst)),
        new std::string(kVid));
    columns->addColumn(column);

    srcVidColName_ = vctx_->anonColGen()->getCol();
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        column =
            new YieldColumn(new InputPropertyExpression(new std::string(kVid)),
                            new std::string(srcVidColName_));
        columns->addColumn(column);
    }

    project = Project::make(qctx_, gn, columns);
    project->setInputVar(gn->outputVar());
    project->setColNames(deduceColNames(columns));
    VLOG(1) << project->outputVar();

    auto* dedupDstVids = Dedup::make(qctx_, project);
    dedupDstVids->setInputVar(project->outputVar());
    dedupDstVids->setOutputVar(outputVar);
    dedupDstVids->setColNames(project->colNames());
    return dedupDstVids;
}

PlanNode* TraversalValidator::buildRuntimeInput() {
    auto pool = qctx_->objPool();
    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(from_.srcRef->clone().release(), new std::string(kVid));
    columns->addColumn(column);
    auto* project = Project::make(qctx_, nullptr, columns);
    if (from_.fromType == kVariable) {
        project->setInputVar(from_.userDefinedVarName);
    }
    project->setColNames({ kVid });
    VLOG(1) << project->outputVar() << " input: " << project->inputVar();
    src_ = pool->add(new InputPropertyExpression(new std::string(kVid)));

    auto* dedupVids = Dedup::make(qctx_, project);
    dedupVids->setInputVar(project->outputVar());
    dedupVids->setColNames(project->colNames());

    projectStartVid_ = project;
    return dedupVids;
}

Expression* TraversalValidator::buildNStepLoopCondition(uint32_t steps) const {
    VLOG(1) << "steps: " << steps;
    // ++loopSteps{0} <= steps
    auto loopSteps = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setValue(loopSteps, 0);
    return qctx_->objPool()->add(new RelationalExpression(
        Expression::Kind::kRelLE,
        new UnaryExpression(
            Expression::Kind::kUnaryIncr,
            new VersionedVariableExpression(new std::string(loopSteps), new ConstantExpression(0))),
        new ConstantExpression(static_cast<int32_t>(steps))));
}

}  // namespace graph
}  // namespace nebula
