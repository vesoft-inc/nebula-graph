/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchVertexIdSeekPlanner.h"

#include "planner/planners/MatchSolver.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {
bool MatchVertexIdSeekPlanner::match(AstContext* astCtx) {
    if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
        return false;
    }
    auto* matchCtx = static_cast<MatchAstContext*>(astCtx);

    auto& head = matchCtx->nodeInfos[0];
    if (matchCtx->filter == nullptr) {
        return false;
    }

    if (head.label != nullptr) {
        // TODO: Only support all tags for now.
        return false;
    }

    if (!matchCtx->edgeInfos.empty()) {
        // TODO: Only support simple node patter for now.
        return false;
    }

    auto vidResult = extractVids(matchCtx->filter.get());
    if (!vidResult.ok()) {
        return false;
    }

    matchCtx->ids = vidResult.value();
    return true;
}

StatusOr<const Expression*> MatchVertexIdSeekPlanner::extractVids(
    const Expression *filter) {
    if (filter->kind() == Expression::Kind::kRelIn) {
        const auto *inExpr = static_cast<const RelationalExpression*>(filter);
        if (inExpr->left()->kind() != Expression::Kind::kFunctionCall ||
            inExpr->right()->kind() != Expression::Kind::kConstant) {
            return Status::Error("Not supported expression.");
        }
        const auto *fCallExpr = static_cast<const FunctionCallExpression*>(inExpr->left());
        if (*fCallExpr->name() != "id") {
            return Status::Error("Require id limit.");
        }
        auto *constExpr = const_cast<Expression*>(inExpr->right());
        return constExpr;
    } else if (filter->kind() == Expression::Kind::kRelEQ) {
        const auto *eqExpr = static_cast<const RelationalExpression*>(filter);
        if (eqExpr->left()->kind() != Expression::Kind::kFunctionCall ||
            eqExpr->right()->kind() != Expression::Kind::kConstant) {
            return Status::Error("Not supported expression.");
        }
        const auto *fCallExpr = static_cast<const FunctionCallExpression*>(eqExpr->left());
        if (*fCallExpr->name() != "id") {
            return Status::Error("Require id limit.");
        }
        auto *constExpr = const_cast<Expression*>(eqExpr->right());
        return constExpr;
    } else {
        return Status::Error("Not supported expression.");
    }
}

std::pair<std::string, Expression *> MatchVertexIdSeekPlanner::listToAnnoVarVid(
    const List &list) {
    auto *qctx = matchCtx_->qctx;
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    for (auto &v : list.values) {
        vids.emplace_back(Row({std::move(v)}));
    }

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto *src = qctx->objPool()->makeAndAdd<VariablePropertyExpression>(new std::string(input),
                                                                        new std::string(kVid));
    return std::pair<std::string, Expression *>(input, src);
}

std::pair<std::string, Expression *> MatchVertexIdSeekPlanner::constToAnnoVarVid(
    const Value &v) {
    auto *qctx = matchCtx_->qctx;
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    vids.emplace_back(Row({v}));

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto *src = qctx->objPool()->makeAndAdd<VariablePropertyExpression>(new std::string(input),
                                                                        new std::string(kVid));
    return std::pair<std::string, Expression *>(input, src);
}

StatusOr<SubPlan> MatchVertexIdSeekPlanner::transform(AstContext* astCtx) {
    matchCtx_ = static_cast<MatchAstContext*>(astCtx);

    NG_RETURN_IF_ERROR(buildQueryById());
    NG_RETURN_IF_ERROR(buildProjectVertices());
    NG_RETURN_IF_ERROR(MatchSolver::buildReturn(matchCtx_, subPlan_));
    VLOG(1) << "root: " << subPlan_.root->kind() << " tail: " << subPlan_.tail->kind();
    return subPlan_;
}

Status MatchVertexIdSeekPlanner::buildQueryById() {
    auto* ids = const_cast<Expression*>(matchCtx_->ids);
    QueryExpressionContext dummy;
    const auto& value = ids->eval(dummy);
    std::pair<std::string, Expression*> vidsResult;
    if (value.isList()) {
        vidsResult = listToAnnoVarVid(value.getList());
    } else {
        vidsResult = constToAnnoVarVid(value);
    }

    auto propsResult = buildVertexProp();
    NG_RETURN_IF_ERROR(propsResult);
    auto *ge =
        GetVertices::make(matchCtx_->qctx,
                          nullptr,
                          matchCtx_->space.id,
                          vidsResult.second,
                          std::move(propsResult).value(),
                          {});
    ge->setInputVar(vidsResult.first);
    subPlan_.root = ge;
    subPlan_.tail = ge;
    return Status::OK();
}

Status MatchVertexIdSeekPlanner::buildProjectVertices() {
    auto &srcNodeInfo = matchCtx_->nodeInfos.front();
    auto *columns = matchCtx_->qctx->objPool()->makeAndAdd<YieldColumns>();
    columns->addColumn(new YieldColumn(new VertexExpression()));
    auto *project = Project::make(matchCtx_->qctx, subPlan_.root, columns);
    project->setInputVar(subPlan_.root->outputVar());
    project->setColNames({*srcNodeInfo.alias});
    subPlan_.root = project;
    return Status::OK();
}

StatusOr<std::vector<storage::cpp2::VertexProp>> MatchVertexIdSeekPlanner::buildVertexProp() {
    // list all tag properties
    std::map<TagID, std::shared_ptr<const meta::SchemaProviderIf>> tagsSchema;
    std::unordered_map<std::string, TagID> tags;
    const auto allTagsResult = qctx()->schemaMng()->getAllVerTagSchema(matchCtx_->space.id);
    NG_RETURN_IF_ERROR(allTagsResult);
    const auto allTags = std::move(allTagsResult).value();
    for (const auto &tag : allTags) {
        tagsSchema.emplace(tag.first, tag.second.back());
    }
    for (const auto &tagSchema : tagsSchema) {
        auto tagNameResult = qctx()->schemaMng()->toTagName(matchCtx_->space.id, tagSchema.first);
        NG_RETURN_IF_ERROR(tagNameResult);
        tags.emplace(std::move(tagNameResult).value(), tagSchema.first);
    }

    std::vector<storage::cpp2::VertexProp> vProps;
    for (const auto &tagSchema : tagsSchema) {
        storage::cpp2::VertexProp vProp;
        vProp.set_tag(tagSchema.first);
        auto tagNameResult = qctx()->schemaMng()->toTagName(matchCtx_->space.id, tagSchema.first);
        NG_RETURN_IF_ERROR(tagNameResult);
        auto tagName = std::move(tagNameResult).value();
        std::vector<std::string> props;
        for (std::size_t i = 0; i < tagSchema.second->getNumFields(); ++i) {
            props.emplace_back(tagSchema.second->getFieldName(i));
        }
        vProp.set_props(std::move(props));
        vProps.emplace_back(std::move(vProp));
    }
    return vProps;
}

}  // namespace graph
}  // namespace nebula
