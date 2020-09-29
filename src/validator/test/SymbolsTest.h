/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class SymbolsTest : public ValidatorTestBase {
};

TEST_F(SymbolsTest, Go) {
    {
        std::string query = "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
                            "id | GO 2 STEPS FROM $-.id OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.status());
        auto qctx = std::move(status).value();
        EXPECT_NE(qctx, nullptr);
    }
}
}  // namespace graph
}  // namespace nebula
