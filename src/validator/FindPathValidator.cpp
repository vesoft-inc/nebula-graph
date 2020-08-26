/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FindPathValidator.h"

namespace nebula {
namespace graph {
Status FindPathValidator::validateImpl() {
    auto fpSentence = static_cast<FindPathSentence*>(sentence_);
    isShortest_ = fpSentence->isShortest();

    NG_RETURN_IF_ERROR(validateStarts(fpSentence->from(), from_));
    NG_RETURN_IF_ERROR(validateStarts(fpSentence->from(), to_));
    NG_RETURN_IF_ERROR(validateOver(fpSentence->over(), over_));
    NG_RETURN_IF_ERROR(validateStep(fpSentence->step(), step_));
    return Status::OK();
}

Status FindPathValidator::validateStarts(const VerticesClause* clause, Starts& starts) {
    if (clause == nullptr) {
        return Status::Error("From clause nullptr.");
    }
    if (clause->isRef()) {
        auto* src = clause->ref();
        if (src->kind() != Expression::Kind::kInputProperty
                && src->kind() != Expression::Kind::kVarProperty) {
            return Status::Error(
                    "`%s', Only input and variable expression is acceptable"
                    " when starts are evaluated at runtime.", src->toString().c_str());
        } else {
            starts.fromType = src->kind() == Expression::Kind::kInputProperty ? kPipe : kVariable;
            auto type = deduceExprType(src);
            if (!type.ok()) {
                return type.status();
            }
            if (type.value() != Value::Type::STRING) {
                std::stringstream ss;
                ss << "`" << src->toString() << "', the srcs should be type of string, "
                   << "but was`" << type.value() << "'";
                return Status::Error(ss.str());
            }
            starts.srcRef = src;
            auto* symPropExpr = static_cast<PropertyExpression*>(src);
            if (starts.fromType == kVariable) {
                starts.userDefinedVarName = *(symPropExpr->sym());
            }
        }
    } else {
        auto vidList = clause->vidList();
        QueryExpressionContext ctx(qctx_->ectx(), nullptr);
        for (auto* expr : vidList) {
            if (!evaluableExpr(expr)) {
                return Status::Error("`%s' is not an evaluable expression.",
                        expr->toString().c_str());
            }
            auto vid = expr->eval(ctx);
            if (!vid.isStr()) {
                return Status::Error("Vid should be a string.");
            }
            starts.vids.emplace_back(std::move(vid));
        }
    }
    return Status::OK();
}

Status FindPathValidator::validateOver(const OverClause* clause, Over& over) {
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
            VLOG(1) << "et: " << edgeType.value();
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

Status FindPathValidator::validateStep(const StepClause* clause, Step& step) {
    if (clause == nullptr) {
        return Status::Error("Step clause nullptr.");
    }
    if (clause->isMToN()) {
        auto* mToN = qctx_->objPool()->makeAndAdd<StepClause::MToN>();
        mToN->mSteps = clause->mToN()->mSteps;
        mToN->nSteps = clause->mToN()->nSteps;
        if (mToN->mSteps == 0) {
            mToN->mSteps = 1;
        }
        if (mToN->nSteps < mToN->mSteps) {
            return Status::Error("`%s', upper bound steps should be greater than lower bound.",
                                 clause->toString().c_str());
        }
        if (mToN->mSteps == mToN->nSteps) {
            steps_ = mToN->mSteps;
            return Status::OK();
        }
        step.mToN = mToN;
    } else {
        auto steps = clause->steps();
        if (steps == 0) {
            return Status::Error("Only accpet positive number steps.");
        }
        step.steps = steps;
    }
    return Status::OK();
}

Status FindPathValidator::toPlan() {
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
