/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_PIPEVALIDATOR_H_
#define VALIDATOR_PIPEVALIDATOR_H_

#include "base/Base.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class PipeValidator final : public Validator {
public:
    explicit PipeValidator(Sentence* sentence, ValidateContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    /**
     * Connect the execution plans for the left and right subtrees.
     * For example: Go FROM id1 OVER e1 YIELD e1._dst AS id | GO FROM $-.id OVER e2;
     * The plan of left subtree's would be:
     *  Start -> GetNeighbor(id1, e1) -> Project(_dst AS id)
     * and the right would be:
     *  Start -> GetNeighbor($-.id, e2) -> Project(_dst)
     * After connecting, it would be:
     *  Start -> GetNeighbor(id1, e1) -> Project(_dst AS id) ->
     *        GetNeighbor($-.id, e2) -> Project(_dst)
     */
    Status toPlan() override;

private:
    std::unique_ptr<Validator>  lValidator_;
    std::unique_ptr<Validator>  rValidator_;
};
}  // namespace graph
}  // namespace nebula
#endif
