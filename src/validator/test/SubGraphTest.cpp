/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "ValidatorTest.h"

namespace nebula {
namespace graph {

TEST_F(ValidatorTest, Subgraph) {
    {
        std::string query = "GET SUBGRAPH 3 STEPS FROM 1";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        // TODO: Check the plan.
    }
}

}   // namespace graph
}   // namespace nebula
