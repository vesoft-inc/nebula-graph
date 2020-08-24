/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GETSUBGRAPHVALIDATOR_H_
#define VALIDATOR_GETSUBGRAPHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"
#include "parser/Clauses.h"

namespace nebula {
namespace graph {
class GetSubgraphValidator final : public TraversalValidator {
public:
    GetSubgraphValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {}

private:
    GraphStatus validateImpl() override;

    GraphStatus toPlan() override;

    GraphStatus validateInBound(InBoundClause* in);

    GraphStatus validateOutBound(OutBoundClause* out);

    GraphStatus validateBothInOutBound(BothInOutClause* out);

private:
    std::unordered_set<EdgeType>                edgeTypes_;
};
}  // namespace graph
}  // namespace nebula
#endif
