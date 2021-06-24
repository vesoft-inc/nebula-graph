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

    if (!exprProps_.srcTagProps().empty()) {
        return Status::SemanticError("Only support input and variable in Subgraph sentence.");
    }
    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable in Subgraph sentence.");
    }
    outputs_.emplace_back("vertices", Value::Type::LIST);
    outputs_.emplace_back("edges", Value::Type::LIST);

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
                                  Expression::Kind::kSrcProperty,
                                  Expression::Kind::kVarProperty,
                                  Expression::Kind::kInputProperty})) {
        return Status::SemanticError("Not support `%s' in where sentence.",
                                     filter_->toString().c_str());
    }
    if (ExpressionUtils::findAny(filter_, {Expression::Kind::kDstProperty})) {
        dstFilter_ = true;
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
    if (!exprProps_.edgeProps().empty()) {
        return wantedEdgeProps();
    }
    std::vector<EdgeType> edgeTypes(edgeTypes_.begin(), edgeTypes_.end());
    auto edgeProps = SchemaUtil::getEdgeProps(qctx_, space_, std::move(edgeTypes), withProp_);
    NG_RETURN_IF_ERROR(edgeProps);
    return edgeProps;
}

std::unique_ptr<std::vector<EdgeProp>> GetSubgraphValidator::wantedEdgeProps() {
    auto edgePropsPtr = std::make_unique<std::vector<EdgeProp>>();
    edgePropsPtr->reserve(edgeTypes_.size());

    const auto& edgeProps = exprProps_.edgeProps();
    for (const auto edgeType : edgeTypes_) {
        EdgeProp ep;
        ep.set_type(edgeType);
        const auto& found = edgeProps.find(std::abs(edgeType));
        if (found != edgeProps.end()) {
            std::set<std::string> props(found->second.begin(), found->second.end());
            props.emplace(kType);
            props.emplace(kRank);
            props.emplace(kDst);
            ep.set_props(std::vector<std::string>(props.begin(), props.end()));
        } else {
            ep.set_props({kType, kRank, kDst});
        }
        edgePropsPtr->emplace_back(std::move(ep));
    }
    return edgePropsPtr;
}

StatusOr<std::unique_ptr<std::vector<VertexProp>>> GetSubgraphValidator::buildVertexProp() {
    const auto& dstTagProps = exprProps_.dstTagProps();
    const auto allTagsResult = qctx()->schemaMng()->getAllLatestVerTagSchema(space_.id);
    NG_RETURN_IF_ERROR(allTagsResult);
    const auto allTags = std::move(allTagsResult).value();

    auto vertexProps = std::make_unique<std::vector<VertexProp>>();
    vertexProps->reserve(allTags.size());

    for (const auto& tag : allTags) {
        VertexProp vp;
        vp.set_tag(tag.first);

        if (withProp_) {
            std::vector<std::string> props;
            for (std::size_t i = 0; i < tag.second->getNumFields(); ++i) {
                props.emplace_back(tag.second->getFieldName(i));
            }
            vp.set_props(std::move(props));
            vertexProps->emplace_back(std::move(vp));
        } else if (!dstTagProps.empty()) {
            const auto& found = dstTagProps.find(tag.first);
            if (found != dstTagProps.end()) {
                vp.set_props(std::vector<std::string>(found->second.begin(), found->second.end()));
                vertexProps->emplace_back(std::move(vp));
            }
        }
    }
    return vertexProps;
}

Status GetSubgraphValidator::zeroStep(PlanNode* depend, const std::string& inputVar) {
    auto& space = vctx_->whichSpace();
    std::unique_ptr<std::vector<nebula::storage::cpp2::Expr>> exprs;
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

    auto edgeProps = buildEdgeProps();
    NG_RETURN_IF_ERROR(edgeProps);
    auto* gn = GetNeighbors::make(qctx_, bodyStart, space.id);
    gn->setSrc(from_.src);
    if (withProp_ || !exprProps_.dstTagProps().empty()) {
        auto vertexProps = buildVertexProp();
        NG_RETURN_IF_ERROR(vertexProps);
        gn->setVertexProps(std::move(vertexProps).value());
    }
    gn->setEdgeProps(std::move(edgeProps).value());
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

    // getVertices
    PlanNode* dcDep = loop;
    if (dstFilter_) {
        std::unique_ptr<std::vector<nebula::storage::cpp2::Expr>> exprs;
        auto vertexProps = buildVertexProp();
        NG_RETURN_IF_ERROR(vertexProps);
        auto* src = qctx_->objPool()->makeAndAdd<VariablePropertyExpression>("*", kVid);
        auto* gv = GetVertices::make(
            qctx_, loop, space.id, src, std::move(vertexProps).value(), std::move(exprs));
        gv->setInputVar(subgraph->outputVar());

        auto* filter = Filter::make(qctx_, gv, filter_);
        dcDep = filter;
    }

    auto* dc = DataCollect::make(qctx_, DataCollect::DCKind::kSubgraph);
    dc->addDep(dcDep);
    if (dstFilter_) {
        // tagfilter
        dc->setInputVars({dep->outputVar(), dcDep->outputVar()});
    } else {
        // edgefilter
        dc->setInputVars({dep->outputVar(), subgraph->outputVar()});
    }
    dc->setColNames({"vertices", "edges"});
    root_ = dc;
    tail_ = projectStartVid_ != nullptr ? projectStartVid_ : loop;

    outputs_.emplace_back(kVertices, Value::Type::VERTEX);
    outputs_.emplace_back(kEdges, Value::Type::EDGE);
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
