/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_MATCHVALIDATOR_H_
#define VALIDATOR_MATCHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"
#include "util/AnonVarGenerator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

class MatchValidator final : public TraversalValidator {
public:
    MatchValidator(Sentence *sentence, QueryContext *context)
        : TraversalValidator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validatePath(const MatchPath *path);

    Status validateFilter(const WhereClause *filter);

    Status validateReturn(MatchReturn *ret);

    const Expression* rewriteExpression(const Expression *expr);

    Status detectOrigin();

private:
    struct NodeInfo {
        TagID                                   tid{0};
        bool                                    anonymous{false};
        const std::string                      *label{nullptr};
        const std::string                      *alias{nullptr};
        const MapExpression                    *props{nullptr};
    };

    struct EdgeInfo {
        bool                                    anonymous{false};
        EdgeType                                edgeType{0};
        MatchEdge::Direction                    direction{MatchEdge::Direction::OUT_EDGE};
        const std::string                      *type{nullptr};
        const std::string                      *alias{nullptr};
        const MapExpression                    *props{nullptr};
    };

    enum AliasType {
        kNode, kEdge, kPath
    };

private:
    bool                                        originateFromNode_{true};
    size_t                                      originIndex_{0};
    std::vector<NodeInfo>                       nodeInfos_;
    std::vector<EdgeInfo>                       edgeInfos_;
    std::unordered_map<std::string, AliasType>  aliases_;
    AnonVarGenerator                            anon_;
};

}   // namespace graph
}   // namespace nebula

#endif  // VALIDATOR_MATCHVALIDATOR_H_
