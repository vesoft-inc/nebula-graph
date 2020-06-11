/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include <gtest/gtest.h>

#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "context/QueryContext.h"
#include "planner/ExecutionPlan.h"
#include "context/ValidateContext.h"

namespace nebula {
namespace graph {
class ValidatorTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        auto session = new ClientSession(0);
        session->setSpace("test", 0);
        session_.reset(session);
        // TODO: Need AdHocSchemaManager here.
    }

    void SetUp() override {
    }

    void TearDown() override {
    }

    std::unique_ptr<QueryContext> buildContext();

protected:
    static std::shared_ptr<ClientSession>      session_;
    static meta::SchemaManager*                schemaMng_;
};

}  // namespace graph
}  // namespace nebula
