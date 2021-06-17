/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"
#include <memory>

#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/VertexExpression.h"
#include "context/QueryExpressionContext.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"
#include "planner/plan/Algo.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

Status GetSubgraphValidator::validateImpl() {
    auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);
    withProp_ = gsSentence->withProp();

    NG_RETURN_IF_ERROR(validateStep(gsSentence->step(), steps_));
    NG_RETURN_IF_ERROR(validateStarts(gsSentence->from(), from_));
    NG_RETURN_IF_ERROR(validateInBound(gsSentence->in()));
    NG_RETURN_IF_ERROR(validateOutBound(gsSentence->out()));
    NG_RETURN_IF_ERROR(validateBothInOutBound(gsSentence->both()));
    NG_RETURN_IF_ERROR(validateWhere(gsSentence->where()));

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty()) {
        return Status::SemanticError("Only support input and variable in Subgraph sentence.");
    }
    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable in Subgraph sentence.");
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
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

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
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

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
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            auto v = et.value();
            edgeTypes_.emplace(v);
            v = -v;
            edgeTypes_.emplace(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateWhere(WhereClause* where) {
    if (where == nullptr) {
        return Status::OK();
    }

    filter_ = where->filter();
    if (ExpressionUtils::findAny(filter_,
                                 {Expression::Kind::kAggregate,
                                  Expression::Kind::kDstProperty,
                                  Expression::Kind::kSrcProperty,
                                  Expression::Kind::kVarProperty,
                                  Expression::Kind::kInputProperty})) {
        return Status::SemanticError("Not support `%s' in where sentence.",
                                     filter_->toString().c_str());
    }
    where->setFilter(ExpressionUtils::rewriteLabelAttr2EdgeProp(filter_));
    filter_ = where->filter();
    auto typeStatus = deduceExprType(filter_);
    NG_RETURN_IF_ERROR(typeStatus);
    auto type = typeStatus.value();
    if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE &&
        type != Value::Type::__EMPTY__) {
        std::stringstream ss;
        ss << "`" << filter_->toString() << "', expected Boolean, "
           << "but was `" << type << "'";
        return Status::SemanticError(ss.str());
    }

    NG_RETURN_IF_ERROR(deduceProps(filter_, exprProps_));
    return Status::OK();
}

StatusOr<std::unique_ptr<std::vector<EdgeProp>>> GetSubgraphValidator::buildEdgeProps() {
    if (edgeTypes_.empty()) {
        const auto allEdgesSchema = qctx_->schemaMng()->getAllLatestVerEdgeSchema(space_.id);
        NG_RETURN_IF_ERROR(allEdgesSchema);
        const auto allEdges = std::move(allEdgesSchema).value();
        for (const auto& edge : allEdges) {
            edgeTypes_.emplace(edge.first);
            edgeTypes_.emplace(-edge.first);
        }
    }
    std::vector<EdgeType> edgeTypes(edgeTypes_.begin(), edgeTypes_.end());
    auto edgeProps = SchemaUtil::getEdgeProps(qctx_, space_, std::move(edgeTypes), withProp_);
    NG_RETURN_IF_ERROR(edgeProps);
    return edgeProps;
}

Status GetSubgraphValidator::zeroStep(PlanNode* depend, const std::string& inputVar) {
    auto& space = vctx_->whichSpace();
    std::unique_ptr<std::vector<Expr>> exprs;
    auto vertexProps = SchemaUtil::getAllVertexProp(qctx_, space, withProp_);
    NG_RETURN_IF_ERROR(vertexProps);
    auto* getVertex = GetVertices::make(qctx_,
                                        depend,
                                        space.id,
                                        from_.src,
                                        std::move(vertexProps).value(),
                                        std::move(exprs),
                                        true);
    getVertex->setInputVar(inputVar);

    auto var = vctx_->anonVarGen()->getVar();
    auto* pool = qctx_->objPool();
    auto* column = VertexExpression::make(pool);
    auto* func = AggregateExpression::make(pool, "COLLECT", column, false);

    auto* collectVertex = Aggregate::make(qctx_, getVertex, {}, {func});
    collectVertex->setColNames({kVertices});
    outputs_.emplace_back(kVertices, Value::Type::VERTEX);
    root_ = collectVertex;
    tail_ = projectStartVid_ != nullptr ? projectStartVid_ : getVertex;
    return Status::OK();
}

Status GetSubgraphValidator::toPlan() {
    auto& space = vctx_->whichSpace();
    auto* bodyStart = StartNode::make(qctx_);

    std::string startVidsVar;
    PlanNode* loopDep = nullptr;
    if (!from_.vids.empty() && from_.originalSrc == nullptr) {
        buildConstantInput(from_, startVidsVar);
    } else {
        loopDep = buildRuntimeInput(from_, projectStartVid_);
        startVidsVar = loopDep->outputVar();
    }

    if (steps_.steps() == 0) {
        return zeroStep(loopDep == nullptr ? bodyStart : loopDep, startVidsVar);
    }

    auto* gn = GetNeighbors::make(qctx_, bodyStart, space.id);
    gn->setSrc(from_.src);
    if (withProp_) {
        auto vertexPropsResult = buildVertexProp();
        NG_RETURN_IF_ERROR(vertexPropsResult);
        gn->setVertexProps(std::move(vertexPropsResult).value());
    }
    auto edgePropsResult = buildEdgeProps();
    NG_RETURN_IF_ERROR(edgePropsResult);
    gn->setEdgeProps(
        std::make_unique<std::vector<storage::cpp2::EdgeProp>>(*edgePropsResult.value()));
    gn->setInputVar(startVidsVar);

    PlanNode* dep = gn;
    if (filter_ != nullptr) {
        auto* filter = Filter::make(qctx_, gn, filter_);
        dep = filter;
    }

    auto oneMoreStepOutput = vctx_->anonVarGen()->getVar();
    auto* subgraph = Subgraph::make(qctx_, dep, oneMoreStepOutput, loopSteps_, steps_.steps() + 1);
    subgraph->setOutputVar(startVidsVar);
    subgraph->setColNames({nebula::kVid});

    auto* loopCondition = buildExpandCondition(gn->outputVar(), steps_.steps());
    auto* loop = Loop::make(qctx_, loopDep, subgraph, loopCondition);

    auto* dc = DataCollect::make(qctx_, DataCollect::DCKind::kSubgraph);
    dc->addDep(loop);
    dc->setInputVars({gn->outputVar(), subgraph->outputVar()});
    dc->setColNames({"_vertices", "_edges"});
    root_ = dc;
    tail_ = projectStartVid_ != nullptr ? projectStartVid_ : loop;

    outputs_.emplace_back(kVertices, Value::Type::VERTEX);
    outputs_.emplace_back(kEdges, Value::Type::EDGE);
    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
