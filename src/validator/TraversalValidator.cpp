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

Status TraversalValidator::validateStarts(const VerticesClause* clause, Starts& starts) {
    if (clause == nullptr) {
        return Status::SemanticError("From clause nullptr.");
    }
    if (clause->isRef()) {
        auto* src = clause->ref();
        if (src->kind() != Expression::Kind::kInputProperty
                && src->kind() != Expression::Kind::kVarProperty) {
            return Status::SemanticError(
                    "`%s', Only input and variable expression is acceptable"
                    " when starts are evaluated at runtime.", src->toString().c_str());
        }
        starts.fromType = src->kind() == Expression::Kind::kInputProperty ? kPipe : kVariable;
        auto type = deduceExprType(src);
        if (!type.ok()) {
            return type.status();
        }
        auto vidType = space_.spaceDesc.vid_type.get_type();
        if (type.value() != SchemaUtil::propTypeToValueType(vidType)) {
            std::stringstream ss;
            ss << "`" << src->toString() << "', the srcs should be type of "
                << meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(vidType) << ", but was`"
                << type.value() << "'";
            return Status::SemanticError(ss.str());
        }
        starts.originalSrc = src;
        auto* propExpr = static_cast<PropertyExpression*>(src);
        if (starts.fromType == kVariable) {
            starts.userDefinedVarName = *(propExpr->sym());
            userDefinedVarNameList_.emplace(starts.userDefinedVarName);
        }
        starts.firstBeginningSrcVidColName = *(propExpr->prop());
    } else {
        auto vidList = clause->vidList();
        QueryExpressionContext ctx;
        for (auto* expr : vidList) {
            if (!evaluableExpr(expr)) {
                return Status::SemanticError("`%s' is not an evaluable expression.",
                        expr->toString().c_str());
            }
            auto vid = expr->eval(ctx(nullptr));
            auto vidType = space_.spaceDesc.vid_type.get_type();
            if (!SchemaUtil::isValidVid(vid, vidType)) {
                std::stringstream ss;
                ss << "Vid should be a " << meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(vidType);
                return Status::SemanticError(ss.str());
            }
            starts.vids.emplace_back(std::move(vid));
            startVidList_->add(expr->clone().release());
        }
    }
    return Status::OK();
}

Status TraversalValidator::validateOver(const OverClause* clause, Over& over) {
    if (clause == nullptr) {
        return Status::SemanticError("Over clause nullptr.");
    }

    over.direction = clause->direction();
    auto* schemaMng = qctx_->schemaMng();
    if (clause->isOverAll()) {
        auto allEdgeStatus = schemaMng->getAllEdge(space_.id);
        NG_RETURN_IF_ERROR(allEdgeStatus);
        auto edges = std::move(allEdgeStatus).value();
        if (edges.empty()) {
            return Status::SemanticError("No edge type found in space `%s'",
                    space_.name.c_str());
        }
        for (auto edge : edges) {
            auto edgeType = schemaMng->toEdgeType(space_.id, edge);
            if (!edgeType.ok()) {
                return Status::SemanticError("`%s' not found in space [`%s'].",
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
                return Status::SemanticError("%s not found in space [%s].",
                        edgeName.c_str(), space_.name.c_str());
            }
            over.edgeTypes.emplace_back(edgeType.value());
        }
    }
    return Status::OK();
}

Status TraversalValidator::validateStep(const StepClause* clause, Steps& step) {
    if (clause == nullptr) {
        return Status::SemanticError("Step clause nullptr.");
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
            return Status::SemanticError(
                "`%s', upper bound steps should be greater than lower bound.",
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

void TraversalValidator::buildConstantInput(Starts& starts, std::string& startVidsVar) {
    startVidsVar = vctx_->anonVarGen()->getVar();
    DataSet ds;
    ds.colNames.emplace_back(kVid);
    for (auto& vid : starts.vids) {
        Row row;
        row.values.emplace_back(vid);
        ds.rows.emplace_back(std::move(row));
    }
    qctx_->ectx()->setResult(startVidsVar, ResultBuilder().value(Value(std::move(ds))).finish());

    starts.src =
        new VariablePropertyExpression(new std::string(startVidsVar), new std::string(kVid));
    qctx_->objPool()->add(starts.src);
}

PlanNode* TraversalValidator::buildRuntimeInput(Starts& starts, PlanNode*& projectStartVid) {
    auto pool = qctx_->objPool();
    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(starts.originalSrc->clone().release(), new std::string(kVid));
    columns->addColumn(column);

    auto* project = Project::make(qctx_, nullptr, columns);
    if (starts.fromType == kVariable) {
        project->setInputVar(starts.userDefinedVarName);
    }
    project->setColNames({kVid});
    VLOG(1) << project->outputVar() << " input: " << project->inputVar();
    starts.src = pool->add(new InputPropertyExpression(new std::string(kVid)));

    auto* dedupVids = Dedup::make(qctx_, project);
    dedupVids->setInputVar(project->outputVar());
    dedupVids->setColNames(project->colNames());

    projectStartVid = project;
    return dedupVids;
}

Expression* TraversalValidator::buildNStepLoopCondition(uint32_t steps) const {
    VLOG(1) << "steps: " << steps;
    // ++loopSteps{0} <= steps
    qctx_->ectx()->setValue(loopSteps_, 0);
    return qctx_->objPool()->add(new RelationalExpression(
        Expression::Kind::kRelLE,
        new UnaryExpression(Expression::Kind::kUnaryIncr,
                            new VariableExpression(new std::string(loopSteps_))),
        new ConstantExpression(static_cast<int32_t>(steps))));
}

}  // namespace graph
}  // namespace nebula
