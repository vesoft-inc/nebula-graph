/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_SETVALIDATOR_H_
#define VALIDATOR_SETVALIDATOR_H_

#include "validator/Validator.h"

namespace nebula {
namespace graph {
class SetValidator final : public Validator {
public:
    explicit SetValidator(Sentence* sentence) : Validator(sentence) {};

private:
    Status validateImpl() override;

    /**
     * Merge the starting node of the execution plan
     * for the left and right subtrees.
     * For example: Go FROM id1 OVER e1 UNION GO FROM id2 OVER e2;
     * The plan of left subtree's would be:
     *  Start -> GetNeighbor(id1, e1) -> Project(_dst)
     * and the right would be:
     *  Start -> GetNeighbor(id2, e2) -> Project(_dst)
     * After merging, it would be:
     *  Start -> GetNeighbor(id1, e1) -> Project(_dst) -|
     *        |-> GetNeighbor(id2, e2) -> Project(_dst) -> Union
     */
    Status toPlan() override;

private:
    std::unique_ptr<Validator>  lValidator_;
    std::unique_ptr<Validator>  rValidator_;
    SetSentence::Operator       op_;
    bool                        distinct_;
};
}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_SETVALIDATOR_H_
