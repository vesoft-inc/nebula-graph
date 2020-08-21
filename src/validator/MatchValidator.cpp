/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/MatchValidator.h"
#include "visitor/RewriteMatchLabelVisitor.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {

Status MatchValidator::toPlan() {
    auto status = Status::OK();
    do {
        status = buildScanNode();
        if (!status.ok()) {
            break;
        }
        if (!edgeInfos_.empty()) {
            status = buildSteps();
            if (!status.ok()) {
                break;
            }
        }
        status = buildGetTailVertices();
        if (!status.ok()) {
            break;
        }
        if (!edgeInfos_.empty()) {
            status = buildTailJoin();
            if (!status.ok()) {
                break;
            }
        }
        status = buildFilter();
        if (!status.ok()) {
            break;
        }
        status = buildReturn();
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}


Status MatchValidator::validateImpl() {
    auto *sentence = static_cast<MatchSentence*>(sentence_);
    NG_RETURN_IF_ERROR(validatePath(sentence->path()));
    if (sentence->where() != nullptr) {
        NG_RETURN_IF_ERROR(validateFilter(sentence->where()->filter()));
    }
    NG_RETURN_IF_ERROR(validateReturn(sentence->ret()));
    return Status::OK();
}


Status MatchValidator::validatePath(const MatchPath *path) {
    auto *sm = qctx_->schemaMng();
    auto steps = path->steps();

    nodeInfos_.resize(steps + 1);
    edgeInfos_.resize(steps);
    for (auto i = 0u; i <= steps; i++) {
        auto *node = path->node(i);
        auto *label = node->label();
        auto *alias = node->alias();
        auto *props = node->props();
        auto anonymous = false;
        if (label != nullptr) {
            auto tid = sm->toTagID(space_.id, *label);
            if (!tid.ok()) {
                return Status::Error("`%s': Unknown tag", label->c_str());
            }
            nodeInfos_[i].tid = tid.value();
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = saveObject(new std::string(anon_->getVar()));
        }
        if (!aliases_.emplace(*alias, kNode).second) {
            return Status::Error("`%s': Redefined alias", alias->c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            filter = createSubFilter(*alias, props);
        }
        nodeInfos_[i].anonymous = anonymous;
        nodeInfos_[i].label = label;
        nodeInfos_[i].alias = alias;
        nodeInfos_[i].props = props;
        nodeInfos_[i].filter = filter;
    }

    for (auto i = 0u; i < steps; i++) {
        auto *edge = path->edge(i);
        auto *type = edge->type();
        auto *alias = edge->alias();
        auto *props = edge->props();
        auto direction = edge->direction();
        auto anonymous = false;
        if (direction != Direction::OUT_EDGE) {
            return Status::SemanticError("Only outbound traversal supported");
        }
        if (type != nullptr) {
            auto etype = sm->toEdgeType(space_.id, *type);
            if (!etype.ok()) {
                return Status::SemanticError("`%s': Unknown edge type", type->c_str());
            }
            edgeInfos_[i].edgeType = etype.value();
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = saveObject(new std::string(anon_->getVar()));
        }
        if (!aliases_.emplace(*alias, kEdge).second) {
            return Status::SemanticError("`%s': Redefined alias", alias->c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            filter = createSubFilter(*alias, props);
        }
        edgeInfos_[i].anonymous = anonymous;
        edgeInfos_[i].direction = direction;
        edgeInfos_[i].type = type;
        edgeInfos_[i].alias = alias;
        edgeInfos_[i].props = props;
        edgeInfos_[i].filter = filter;
    }

    return analyzeStartPoint();
}


Status MatchValidator::validateFilter(const Expression *filter) {
    filter_ = ExpressionUtils::foldConstantExpr(filter);
    NG_RETURN_IF_ERROR(validateAliases({filter_.get()}));

    return Status::OK();
}


Status MatchValidator::validateReturn(MatchReturn *ret) {
    // `RETURN *': return all named nodes or edges
    if (ret->isAll()) {
        auto makeColumn = [] (const std::string &name) {
            auto *expr = new LabelExpression(name);
            auto *alias = new std::string(name);
            return new YieldColumn(expr, alias);
        };

        auto columns = new YieldColumns();
        auto steps = edgeInfos_.size();

        if (!nodeInfos_[0].anonymous) {
            columns->addColumn(makeColumn(*nodeInfos_[0].alias));
        }

        for (auto i = 0u; i < steps; i++) {
            if (!edgeInfos_[i].anonymous) {
                columns->addColumn(makeColumn(*edgeInfos_[i].alias));
            }
            if (!nodeInfos_[i+1].anonymous) {
                columns->addColumn(makeColumn(*nodeInfos_[i+1].alias));
            }
        }

        if (columns->empty()) {
            return Status::SemanticError("`RETURN *' not allowed if there is no alias");
        }

        ret->setColumns(columns);
    }

    // Check all referencing expressions are valid
    std::vector<const Expression*> exprs;
    exprs.reserve(ret->columns()->size());
    for (auto *col : ret->columns()->columns()) {
        exprs.push_back(col->expr());
    }
    NG_RETURN_IF_ERROR(validateAliases(exprs));

    return Status::OK();
}


Status MatchValidator::validateAliases(const std::vector<const Expression*> &exprs) const {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kLabel,
        Expression::Kind::kLabelAttribute
    };

    for (auto *expr : exprs) {
        auto refExprs = ExpressionUtils::collectAll(expr, kinds);
        if (refExprs.empty()) {
            continue;
        }
        for (auto *refExpr : refExprs) {
            auto kind = refExpr->kind();
            const std::string *name = nullptr;
            if (kind == Expression::Kind::kLabel) {
                name = static_cast<const LabelExpression*>(refExpr)->name();
            } else {
                DCHECK(kind == Expression::Kind::kLabelAttribute);
                name = static_cast<const LabelAttributeExpression*>(refExpr)->left()->name();
            }
            DCHECK(name != nullptr);
            if (aliases_.count(*name) != 1) {
                return Status::SemanticError("Alias used but not defined: `%s'", name->c_str());
            }
        }
    }
    return Status::OK();
}


Status MatchValidator::analyzeStartPoint() {
    // TODO(dutor) Originate from either node or edge at any position
    startFromNode_ = true;
    startIndex_ = 0;
    startExpr_ = nullptr;

    auto &head = nodeInfos_[0];

    if (head.label == nullptr) {
        return Status::Error("Head node must have a label");
    }
    if (head.props == nullptr) {
        return Status::Error("Head node must have a filter");
    }

    auto items = head.props->items();
    DCHECK_EQ(items.size(), 1UL);
    auto *indexFilter = new RelationalExpression(Expression::Kind::kRelEQ,
            new TagPropertyExpression(
                new std::string(*head.label),
                new std::string(*items[0].first)),
            items[0].second->clone().release());
    saveObject(indexFilter);
    scanInfo_.filter = indexFilter;
    scanInfo_.schemaId = head.tid;

    return Status::OK();
}


Expression* MatchValidator::createSubFilter(const std::string &alias,
        const MapExpression *map) const {
    DCHECK_NOTNULL(map);
    auto items = map->items();
    DCHECK(!items.empty());
    Expression *root = nullptr;
    root = new RelationalExpression(Expression::Kind::kRelEQ,
            new LabelAttributeExpression(
                new LabelExpression(alias),
                new LabelExpression(*items[0].first)),
            items[0].second->clone().release());
    for (auto i = 1u; i < items.size(); i++) {
        auto *left = root;
        auto *right = new RelationalExpression(Expression::Kind::kRelEQ,
            new LabelAttributeExpression(
                new LabelExpression(alias),
                new LabelExpression(*items[i].first)),
            items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }

    saveObject(root);

    return root;
}


Status MatchValidator::buildScanNode() {
    if (!startFromNode_) {
        return Status::SemanticError("Scan from edge not supported now");
    }
    if (startIndex_ != 0) {
        return Status::SemanticError("Only support scan from the head node");
    }

    using IQC = nebula::storage::cpp2::IndexQueryContext;
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back();
    contexts->back().set_filter(Expression::encode(*scanInfo_.filter));
    auto columns = std::make_unique<std::vector<std::string>>();
    auto scan = IndexScan::make(qctx_,
                                nullptr,
                                space_.id,
                                std::move(contexts),
                                std::move(columns),
                                false,
                                scanInfo_.schemaId);
    tail_ = scan;
    root_ = scan;

    return Status::OK();
}


Status MatchValidator::buildSteps() {
    gnSrcExpr_ = new VariablePropertyExpression(new std::string(),
                                                new std::string(kVid));
    saveObject(gnSrcExpr_);
    NG_RETURN_IF_ERROR(buildStep());
    for (auto i = 1u; i < edgeInfos_.size(); i++) {
        NG_RETURN_IF_ERROR(buildStep());
        NG_RETURN_IF_ERROR(buildStepJoin());
    }

    return Status::OK();
}


Status MatchValidator::buildStep() {
    curStep_++;

    auto &srcNodeInfo = nodeInfos_[curStep_];
    auto &edgeInfo = edgeInfos_[curStep_];
    auto *gn = GetNeighbors::make(qctx_, root_, space_.id);
    gn->setSrc(gnSrcExpr_);
    auto vertexProps = std::make_unique<std::vector<VertexProp>>();
    if (srcNodeInfo.label != nullptr) {
        VertexProp vertexProp;
        vertexProp.set_tag(srcNodeInfo.tid);
        vertexProps->emplace_back(std::move(vertexProp));
    }
    gn->setVertexProps(std::move(vertexProps));
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    if (edgeInfo.type != nullptr) {
        EdgeProp edgeProp;
        edgeProp.set_type(edgeInfo.edgeType);
        edgeProps->emplace_back(std::move(edgeProp));
    }
    gn->setEdgeProps(std::move(edgeProps));
    gn->setEdgeDirection(edgeInfo.direction);

    auto *yields = saveObject(new YieldColumns());
    yields->addColumn(new YieldColumn(new VertexExpression()));
    yields->addColumn(new YieldColumn(new EdgeExpression()));
    auto *project = Project::make(qctx_, gn, yields);
    project->setInputVar(gn->outputVar());
    project->setColNames({*srcNodeInfo.alias, *edgeInfo.alias});

    root_ = project;

    auto rewriter = [this] (const Expression *expr) {
        DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
        return rewrite(static_cast<const LabelAttributeExpression*>(expr));
    };

    if (srcNodeInfo.filter != nullptr) {
        RewriteMatchLabelVisitor visitor(rewriter);
        srcNodeInfo.filter->accept(&visitor);
        auto *node = Filter::make(qctx_, root_, srcNodeInfo.filter);
        node->setInputVar(root_->outputVar());
        node->setColNames(root_->colNames());
        root_ = node;
    }

    if (edgeInfo.filter != nullptr) {
        RewriteMatchLabelVisitor visitor(rewriter);
        edgeInfo.filter->accept(&visitor);
        auto *node = Filter::make(qctx_, root_, edgeInfo.filter);
        node->setInputVar(root_->outputVar());
        node->setColNames(root_->colNames());
        root_ = node;
    }

    gnSrcExpr_ = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(project->outputVar()),
                new std::string(*edgeInfo.alias)),
            new LabelExpression("_dst"));
    saveObject(gnSrcExpr_);

    prevStepRoot_ = thisStepRoot_;
    thisStepRoot_ = root_;

    return Status::OK();
}


Status MatchValidator::buildGetTailVertices() {
    Expression *src = nullptr;
    if (!edgeInfos_.empty()) {
        src = new AttributeExpression(
                new VariablePropertyExpression(
                    new std::string(),
                    new std::string(*edgeInfos_[curStep_].alias)),
                new LabelExpression("_dst"));
    } else {
        src = new VariablePropertyExpression(new std::string(),
                new std::string(kVid));
    }
    saveObject(src);

    auto &nodeInfo = nodeInfos_[curStep_ + 1];
    std::vector<VertexProp> props;
    if (nodeInfo.label != nullptr) {
        VertexProp prop;
        prop.set_tag(nodeInfo.tid);
        props.emplace_back(prop);
    }

    auto *gv = GetVertices::make(qctx_, root_, space_.id, src, std::move(props), {}, true);
    if (thisStepRoot_ != nullptr) {
        gv->setInputVar(thisStepRoot_->outputVar());
    }

    auto *yields = saveObject(new YieldColumns());
    yields->addColumn(new YieldColumn(new VertexExpression()));
    auto *project = Project::make(qctx_, gv, yields);
    project->setInputVar(gv->outputVar());
    project->setColNames({*nodeInfo.alias});

    auto *dedup = Dedup::make(qctx_, project);
    dedup->setInputVar(project->outputVar());
    dedup->setColNames(project->colNames());
    root_ = dedup;

    return Status::OK();
}


Status MatchValidator::buildStepJoin() {
    auto prevStep = curStep_ - 1;
    auto key = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(prevStepRoot_->outputVar()),
                new std::string(*edgeInfos_[prevStep].alias)),
            new LabelExpression("_dst"));
    auto probe = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(thisStepRoot_->outputVar()),
                new std::string(*nodeInfos_[curStep_].alias)),
            new LabelExpression("_vid"));
    auto *join = DataJoin::make(qctx_,
                                root_,
                                {prevStepRoot_->outputVar(), 0},
                                {thisStepRoot_->outputVar(), 0},
                                {key},
                                {probe});
    auto leftColNames = prevStepRoot_->colNames();
    auto rightColNames = thisStepRoot_->colNames();
    std::vector<std::string> colNames;
    colNames.reserve(leftColNames.size() + rightColNames.size());
    for (auto &name : leftColNames) {
        colNames.emplace_back(std::move(name));
    }
    for (auto &name : rightColNames) {
        colNames.emplace_back(std::move(name));
    }
    join->setColNames(std::move(colNames));
    root_ = join;
    thisStepRoot_ = root_;

    return Status::OK();
}


Status MatchValidator::buildTailJoin() {
    auto key = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(thisStepRoot_->outputVar()),
                new std::string(*edgeInfos_[curStep_].alias)),
            new LabelExpression("_dst"));
    auto probe = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(root_->outputVar()),
                new std::string(*nodeInfos_[curStep_ + 1].alias)),
            new LabelExpression("_vid"));
    auto *join = DataJoin::make(qctx_,
                                root_,
                                {thisStepRoot_->outputVar(), 0},
                                {root_->outputVar(), 0},
                                {key},
                                {probe});
    auto colNames = thisStepRoot_->colNames();
    colNames.emplace_back(*nodeInfos_[curStep_ + 1].alias);
    join->setColNames(std::move(colNames));
    root_ = join;

    return Status::OK();
}


Status MatchValidator::buildFilter() {
    auto *sentence = static_cast<MatchSentence*>(sentence_);
    auto *clause = sentence->where();
    if (clause == nullptr) {
        return Status::OK();
    }
    auto *filter = clause->filter();
    auto kind = filter->kind();
    // TODO(dutor) Find a better way to identify where an expr is a boolean one
    if (kind == Expression::Kind::kLabel ||
            kind == Expression::Kind::kLabelAttribute) {
        return Status::SemanticError("Filter should be a bllean expression");
    }

    auto newFilter = filter->clone();
    auto rewriter = [this] (const Expression *expr) {
        if (expr->kind() == Expression::Kind::kLabel) {
            return rewrite(static_cast<const LabelExpression*>(expr));
        } else {
            return rewrite(static_cast<const LabelAttributeExpression*>(expr));
        }
    };
    RewriteMatchLabelVisitor visitor(std::move(rewriter));
    newFilter->accept(&visitor);

    auto *node = Filter::make(qctx_, root_, saveObject(newFilter.release()));
    node->setInputVar(root_->outputVar());
    node->setColNames(root_->colNames());

    root_ = node;

    return Status::OK();
}


Status MatchValidator::buildReturn() {
    auto *sentence = static_cast<MatchSentence*>(sentence_);
    auto *yields = new YieldColumns();
    std::vector<std::string> colNames;

    for (auto *col : sentence->ret()->columns()->columns()) {
        auto kind = col->expr()->kind();
        YieldColumn *newColumn = nullptr;
        if (kind == Expression::Kind::kLabel) {
            auto *label = static_cast<const LabelExpression*>(col->expr());
            newColumn = new YieldColumn(rewrite(label));
        } else if (kind == Expression::Kind::kLabelAttribute) {
            auto *la = static_cast<const LabelAttributeExpression*>(col->expr());
            newColumn = new YieldColumn(rewrite(la));
        } else {
            auto newExpr = col->expr()->clone();
            auto rewriter = [this] (const Expression *expr) {
                if (expr->kind() == Expression::Kind::kLabel) {
                    return rewrite(static_cast<const LabelExpression*>(expr));
                } else {
                    return rewrite(static_cast<const LabelAttributeExpression*>(expr));
                }
            };
            RewriteMatchLabelVisitor visitor(std::move(rewriter));
            newExpr->accept(&visitor);
            newColumn = new YieldColumn(newExpr.release());
        }
        yields->addColumn(newColumn);
        if (col->alias() != nullptr) {
            colNames.emplace_back(*col->alias());
        } else {
            colNames.emplace_back(col->expr()->toString());
        }
    }

    auto *project = Project::make(qctx_, root_, yields);
    project->setInputVar(root_->outputVar());
    project->setColNames(std::move(colNames));
    root_ = project;

    return Status::OK();
}


Expression* MatchValidator::rewrite(const LabelExpression *label) const {
    auto *expr = new VariablePropertyExpression(
            new std::string(),
            new std::string(*label->name()));
    return expr;
}


Expression *MatchValidator::rewrite(const LabelAttributeExpression *la) const {
    auto *expr = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(),
                new std::string(*la->left()->name())),
            new LabelExpression(*la->right()->name()));
    return expr;
}

}   // namespace graph
}   // namespace nebula
