/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/MatchValidator.h"

namespace nebula {
namespace graph {

Status MatchValidator::toPlan() {
    return Status::OK();
}


Status MatchValidator::validateImpl() {
    auto *sentence = static_cast<MatchSentence*>(sentence_);
    NG_RETURN_IF_ERROR(validatePath(sentence->path()));
    NG_RETURN_IF_ERROR(validateFilter(sentence->filter()));
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
            alias = qctx_->objPool()->add(new std::string(anon_.getVar()));
        }
        if (!aliases_.emplace(*alias, kNode).second) {
            return Status::Error("`%s': Redefined alias", alias->c_str());
        }
        nodeInfos_[i].anonymous = anonymous;
        nodeInfos_[i].label = label;
        nodeInfos_[i].alias = alias;
        nodeInfos_[i].props = props;
    }

    for (auto i = 0u; i < steps; i++) {
        auto *edge = path->edge(i);
        auto *type = edge->type();
        auto *alias = edge->alias();
        auto *props = edge->props();
        auto direction = edge->direction();
        auto anonymous = false;
        if (type != nullptr) {
            auto etype = sm->toEdgeType(space_.id, *type);
            if (!etype.ok()) {
                return Status::Error("`%s': Unknown edge type", type->c_str());
            }
            edgeInfos_[i].edgeType = etype.value();
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = qctx_->objPool()->add(new std::string(anon_.getVar()));
        }
        if (!aliases_.emplace(*alias, kEdge).second) {
            return Status::Error("`%s': Redefined alias", alias->c_str());
        }
        edgeInfos_[i].anonymous = anonymous;
        edgeInfos_[i].direction = direction;
        edgeInfos_[i].type = type;
        edgeInfos_[i].alias = alias;
        edgeInfos_[i].props = props;
    }

    return detectOrigin();
}


Status MatchValidator::validateFilter(const WhereClause *filter) {
    UNUSED(filter);
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

        auto columns = qctx_->objPool()->add(new YieldColumns());
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
            return Status::Error("`RETURN *' not allowed if there is no alias");
        }

        ret->setColumns(columns);
    }
    return Status::OK();
}


const Expression* MatchValidator::rewriteExpression(const Expression *expr) {
    UNUSED(expr);
    return nullptr;
}


Status MatchValidator::detectOrigin() {
    auto status = Status::OK();
    // TODO(dutor) Originate from either node or edge at any position
    auto idx = 0;
    auto &nodeInfo = nodeInfos_[idx];

    do {
        if (nodeInfo.label == nullptr) {
            status = Status::Error("Head node must have a label");
            break;
        }
        if (nodeInfo.props == nullptr) {
            status = Status::Error("Head node must has props");
            break;
        }
        // TODO(dutor) Check index-based props
        originateFromNode_ = true;
        originIndex_ = idx;
    } while (false);

    return status;
}

}   // namespace graph
}   // namespace nebula
