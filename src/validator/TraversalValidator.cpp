/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/TraversalValidator.h"
#include "common/expression/VariableExpression.h"

namespace nebula {
namespace graph {

GraphStatus TraversalValidator::validateStarts(const VerticesClause* clause, Starts& starts) {
    if (step == nullptr) {
        return GraphStatus::setInternalError("Step clause nullptr.");
    }
    if (step->isMToN()) {
        auto* mToN = qctx_->objPool()->makeAndAdd<StepClause::MToN>();
        mToN->mSteps = step->mToN()->mSteps;
        mToN->nSteps = step->mToN()->nSteps;

        if (mToN->mSteps == 0 && mToN->nSteps == 0) {
            steps_ = 0;
            return GraphStatus::OK();
        }
        if (mToN->mSteps == 0) {
            mToN->mSteps = 1;
        }
        if (mToN->nSteps < mToN->mSteps) {
            return GraphStatus::setSyntaxError(
                    folly::stringPrintf(
                            "`%s', upper bound steps should be greater than lower bound.",
                            step->toString().c_str()));
        }
        if (mToN->mSteps == mToN->nSteps) {
            steps_ = mToN->mSteps;
            return GraphStatus::OK();
        }
        mToN_ = mToN;
    } else {
        auto steps = step->steps();
>>>>>>> update
        steps_ = steps;
    }
    if (clause->isRef()) {
        auto* src = clause->ref();
        if (src->kind() != Expression::Kind::kInputProperty
                && src->kind() != Expression::Kind::kVarProperty) {
            return GraphStatus::setInvalidExpr(src->toString());
        } else {
            starts.fromType = src->kind() == Expression::Kind::kInputProperty ? kPipe : kVariable;
            auto type = deduceExprType(src);
            if (!type.ok()) {
                return GraphStatus::setInvalidExpr(src->toString());
            }
            if (type.value() != Value::Type::STRING) {
                return GraphStatus::setInvalidVid();
            }
            starts.srcRef = src;
            auto* propExpr = static_cast<PropertyExpression*>(src);
            if (starts.fromType == kVariable) {
                starts.userDefinedVarName = *(propExpr->sym());
            }
            starts.firstBeginningSrcVidColName = *(propExpr->prop());
        }
    } else {
        auto vidList = clause->vidList();
        QueryExpressionContext ctx;
        for (auto* expr : vidList) {
            if (!evaluableExpr(expr)) {
                return GraphStatus::setInvalidExpr(expr->toString());
            }
            auto vid = expr->eval(ctx(nullptr));
            if (!vid.isStr()) {
                return GraphStatus::setInvalidVid();
            }
            starts.vids.emplace_back(std::move(vid));
        }
    }
    return GraphStatus::OK();
}

GraphStatus TraversalValidator::validateOver(const OverClause* clause, Over& over) {
    if (clause == nullptr) {
        return GraphStatus::setInternalError("Over clause nullptr.");
    }

    over.direction = clause->direction();
    auto* schemaMng = qctx_->schemaMng();
    if (clause->isOverAll()) {
        auto allEdgeStatus = schemaMng->getAllEdge(space_.id);
        NG_RETURN_IF_ERROR(allEdgeStatus);
        auto edges = std::move(allEdgeStatus).value();
        if (edges.empty()) {
            return GraphStatus::setSemanticError(
                    folly::stringPrintf("No edge type found in space %s",
                    space_.name.c_str()));
        }
        for (auto edge : edges) {
            auto edgeType = schemaMng->toEdgeType(space_.id, edge);
            if (!edgeType.ok()) {
                return GraphStatus::setSemanticError(
                        folly::stringPrintf("%s not found in space [%s].",
                        edge.c_str(), space_.name.c_str()));
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
                return GraphStatus::setSemanticError(
                        folly::stringPrintf("%s not found in space [%s].",
                        edgeName.c_str(), space_.name.c_str());
            }
            over.edgeTypes.emplace_back(edgeType.value());
        }
    }
    return GraphStatus::OK();
}

GraphStatus TraversalValidator::validateStep(const StepClause* clause, Steps& step) {
    if (clause == nullptr) {
        return GraphStatus::setInternalError("Step clause nullptr.");
    }
    if (clause->isMToN()) {
        auto* mToN = qctx_->objPool()->makeAndAdd<StepClause::MToN>();
        mToN->mSteps = clause->mToN()->mSteps;
        mToN->nSteps = clause->mToN()->nSteps;

        if (mToN->mSteps == 0 && mToN->nSteps == 0) {
            step.steps = 0;
            return GraphStatus::OK();
        }
        if (mToN->mSteps == 0) {
            mToN->mSteps = 1;
        }
        if (mToN->nSteps < mToN->mSteps) {
            return GraphStatus::setSyntaxError(
                    folly::stringPrintf(
                            "`%s', upper bound steps should be greater than lower bound.",
                            step->toString().c_str()));
        }
        if (mToN->mSteps == mToN->nSteps) {
            steps_.steps = mToN->mSteps;
            return GraphStatus::OK();
        }
        step.mToN = mToN;
    } else {
        auto steps = clause->steps();
        step.steps = steps;
    }
    return GraphStatus::OK();
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
    project->setInputVar(gn->varName());
    project->setColNames(deduceColNames(columns));
    VLOG(1) << project->varName();

    auto* dedupDstVids = Dedup::make(qctx_, project);
    dedupDstVids->setInputVar(project->varName());
    dedupDstVids->setOutputVar(outputVar);
    dedupDstVids->setColNames(project->colNames());
    return dedupDstVids;
}

std::string TraversalValidator::buildConstantInput() {
    auto input = vctx_->anonVarGen()->getVar();
    DataSet ds;
    ds.colNames.emplace_back(kVid);
    for (auto& vid : from_.vids) {
        Row row;
        row.values.emplace_back(vid);
        ds.rows.emplace_back(std::move(row));
    }
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(ds))).finish());

    auto* vids = new VariablePropertyExpression(new std::string(input),
                                                new std::string(kVid));
    qctx_->objPool()->add(vids);
    src_ = vids;
    return input;
}

PlanNode* TraversalValidator::buildRuntimeInput() {
    auto pool = qctx_->objPool();
    auto* columns = pool->add(new YieldColumns());
    auto encode = from_.srcRef->encode();
    auto decode = Expression::decode(encode);
    auto* column = new YieldColumn(decode.release(), new std::string(kVid));
    columns->addColumn(column);
    auto* project = Project::make(qctx_, nullptr, columns);
    if (from_.fromType == kVariable) {
        project->setInputVar(from_.userDefinedVarName);
    }
    project->setColNames({ kVid });
    VLOG(1) << project->varName() << " input: " << project->inputVar();
    src_ = pool->add(new InputPropertyExpression(new std::string(kVid)));

    auto* dedupVids = Dedup::make(qctx_, project);
    dedupVids->setInputVar(project->varName());
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
