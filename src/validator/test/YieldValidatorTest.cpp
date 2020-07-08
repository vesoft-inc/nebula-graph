/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/YieldValidator.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class YieldValidatorTest : public ValidatorTestBase {
public:
    void SetUp() override {
        ValidatorTestBase::SetUp();
        expected_ = {PlanNode::Kind::kProject, PlanNode::Kind::kStart};
    }

protected:
    std::vector<PlanNode::Kind> expected_;
};

TEST_F(YieldValidatorTest, Base) {
    {
        std::string query = "YIELD 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#if 0
    {
        std::string query = "YIELD 1+1, '1+1', (int)3.14, (string)(1+1), (string)true";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD \"Hello\", hash(\"Hello\")";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#endif
}

TEST_F(YieldValidatorTest, DISABLED_HashCall) {
    {
        std::string query = "YIELD hash(\"Boris\")";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD hash(123)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD hash(123 + 456)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD hash(123.0)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD hash(!0)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
}

TEST_F(YieldValidatorTest, DISABLED_Logic) {
    {
        std::string query = "YIELD NOT 0 || 0 AND 0 XOR 0";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD !0 OR 0 && 0 XOR 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (NOT 0 || 0) AND 0 XOR 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#if 0
    {
        std::string query = "YIELD 2.5 % 1.2 ^ 1.6";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (5 % 3) ^ 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#endif
}

TEST_F(YieldValidatorTest, DISABLED_InCall) {
    {
        std::string query = "YIELD udf_is_in(1,0,1,2), 123";
        EXPECT_TRUE(checkResult(query, expected_));
    }
}

TEST_F(YieldValidatorTest, YieldPipe) {
    std::string go = "GO FROM \"%s\" OVER like YIELD "
                     "$^.person.name as name, like.start as start";
    {
        auto fmt = go + "| YIELD $-.start";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto fmt = go + "| YIELD $-.start WHERE 1 == 1";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto fmt = go + "| YIELD $-.start WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto fmt = go + "| YIELD $-.*";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto fmt = go + "| YIELD $-.* WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start"}));
    }
#if 0
    {
        auto fmt = go + "| YIELD $-.*, hash(123) as hash WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start", "hash"}));
    }
    {
        auto fmt = go + "| YIELD DISTINCT $-.*, hash(123) as hash WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kDataCollect,
            PlanNode::Kind::kDedup,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start", "hash"}));
    }
    {
        auto fmt = go + "| YIELD DISTINCT hash($-.*) as hash WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        EXPECT_FALSE(checkResult(query));
    }
#endif
    {
        auto fmt = go + "| YIELD DISTINCT 1 + $-.* AS e WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        EXPECT_FALSE(checkResult(query));
    }
}

TEST_F(YieldValidatorTest, YieldVar) {
    std::string var = " $var = GO FROM \"%s\" OVER like YIELD "
                      "$^.person.name as name, like.start as start;";
    {
        auto fmt = var + "YIELD $var.name";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"$var.name"}));
    }
    {
        auto fmt = var + "YIELD $var.name WHERE 1 == 1";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"$var.name"}));
    }
    {
        auto fmt = var + "YIELD $var.name WHERE $var.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto fmt = var + "YIELD $var.*";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start"}));
    }
    {
        auto fmt = var + "YIELD $var.* WHERE $var.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start"}));
    }
#if 0
    {
        auto fmt = var + "YIELD $var.*, hash(123) as hash WHERE $var.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start", "hash"}));
    }
#endif
    {
        auto fmt = var + "YIELD 2 + $var.* AS e WHERE $var.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        EXPECT_FALSE(checkResult(query));
    }
#if 0
    {
        auto fmt = var + "YIELD DISTINCT $var.*, hash(123) as hash WHERE $var.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        expected_ = {
            PlanNode::Kind::kDataCollect,
            PlanNode::Kind::kDedup,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start", "hash"}));
    }
#endif
}

TEST_F(YieldValidatorTest, Error) {
    {
        // Reference input in a single yield sentence is meaningless.
        auto query = "yield $-";
        EXPECT_FALSE(checkResult(query));
    }
    std::string var = " $var = GO FROM \"%s\" OVER like YIELD "
                      "$^.person.name AS name, like.start AS start;";
    {
        // Not support reference input and variable
        auto fmt = var + "YIELD $var.name WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "Not support both input and variable.");
    }
    {
        // Not support reference two different variable
        auto fmt = var + "YIELD $var.name WHERE $var1.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "Only one variable allowed to use.");
    }
    {
        // Reference a non-existed prop is meaningless.
        auto fmt = var + "YIELD $var.abc";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        EXPECT_FALSE(checkResult(query));
    }
    {
        // Reference a non-existed prop is meaningless.
        std::string fmt = "GO FROM \"%s\" OVER like | YIELD $-.abc;";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        EXPECT_FALSE(checkResult(query));
    }
    {
        // Reference properties in single yield sentence is meaningless.
        auto fmt = var + "YIELD $$.person.name";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Only support input and variable in yield sentence.");
    }
    {
        auto fmt = var + "YIELD $^.person.name";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Only support input and variable in yield sentence.");
    }
    {
        auto fmt = var + "YIELD like.start";
        auto query = folly::stringPrintf(fmt.c_str(), "1");
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Only support input and variable in yield sentence.");
    }
}

TEST_F(YieldValidatorTest, AggCall) {
    {
        std::string query = "YIELD COUNT(1), $-.name";
        auto result = checkResult(query);
        // Error would be reported when no input
        // EXPECT_EQ(std::string(result.message()), "SyntaxError: column `name' not exist in
        // input");
        EXPECT_EQ(std::string(result.message()), "`$-.name', not exist prop `name'");
    }
    {
        std::string query = "YIELD COUNT(*), 1+1";
        EXPECT_TRUE(checkResult(query));
    }
    {
        auto *fmt = "GO FROM \"%s\" OVER like "
                    "YIELD $^.person.age AS age, "
                    "like.likeness AS like"
                    "| YIELD COUNT(*), $-.age";
        auto query = folly::stringPrintf(fmt, "1");
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Input columns without aggregation are not supported in YIELD "
                  "statement without GROUP BY, near `$-.age'");
    }
    // Test input
    {
        auto *fmt = "GO FROM \"%s\" OVER like "
                    "YIELD $^.person.age AS age, "
                    "like.likeness AS like"
                    "| YIELD AVG($-.age), SUM($-.like), COUNT(*), 1+1";
        auto query = folly::stringPrintf(fmt, "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    // Yield field has not input
    {
        auto *fmt = "GO FROM \"%s\" OVER like "
                    "| YIELD COUNT(*)";
        auto query = folly::stringPrintf(fmt, "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    // Yield field has not input
    {
        auto *fmt = "GO FROM \"%s\" OVER like "
                    "| YIELD 1";
        auto query = folly::stringPrintf(fmt, "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    // Test var
    {
        auto *fmt = "$var = GO FROM \"%s\" OVER like "
                    "YIELD $^.person.age AS age, "
                    "like.likeness AS like;"
                    "YIELD AVG($var.age), SUM($var.like), COUNT(*)";
        auto query = folly::stringPrintf(fmt, "1");
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
}

}   // namespace graph
}   // namespace nebula
