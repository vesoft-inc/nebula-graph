/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"
#include "parser/TraverseSentences.h"

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
            UNUSED(expr);
            starts_.emplace_back("");
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

            auto et = validateContext_->schemaMng()->toEdgeType(space.second, *e->edge());
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

            auto et = validateContext_->schemaMng()->toEdgeType(space.second, *e->edge());
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

            auto et = validateContext_->schemaMng()->toEdgeType(space.second, *e->edge());
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
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
