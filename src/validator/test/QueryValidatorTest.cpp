/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class QueryValidatorTest : public ValidatorTestBase {
public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(QueryValidatorTest, Subgraph) {
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kStart,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult("GET SUBGRAPH 3 STEPS FROM \"1\"", expected));
}

TEST_F(QueryValidatorTest, TestFirstSentence) {
    auto testFirstSentence = [](const std::string &msg) -> bool {
        return msg.find_first_of("SyntaxError: Could not start with the statement") == 0;
    };

    {
        auto result = checkResult("LIMIT 2, 10");
        EXPECT_FALSE(result);
        EXPECT_TRUE(testFirstSentence(result.message()));
    }
    {
        auto result = checkResult("LIMIT 2, 10 | YIELD 2");
        EXPECT_TRUE(testFirstSentence(result.message()));
    }
    {
        auto result = checkResult("LIMIT 2, 10 | YIELD 2 | YIELD 3");
        EXPECT_TRUE(testFirstSentence(result.message()));
    }
    {
        auto result = checkResult("ORDER BY 1");
        EXPECT_TRUE(testFirstSentence(result.message()));
    }
    {
        auto result = checkResult("GROUP BY 1");
        EXPECT_TRUE(testFirstSentence(result.message()));
    }
}

TEST_F(QueryValidatorTest, Go) {
    {
        std::string query = "GO FROM \"1\" OVER like";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GO 3 STEPS FROM \"1\" OVER like";
        // TODO: implement n steps and test plan.
        EXPECT_FALSE(checkResult(query));
    }
    {
        std::string query = "GO FROM \"1\" OVER like REVERSELY";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like YIELD like.start";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $^.person.name,$^.person.age";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.person.name,$$.person.age";
        // TODO: implement get dst props and test plan.
        EXPECT_FALSE(checkResult(query));
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like WHERE like.likeness > 90";
        EXPECT_TRUE(checkResult(query));
        // TODO
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like WHERE $^.person.name == \"me\"";
        EXPECT_TRUE(checkResult(query));
        // TODO
    }
    {
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.id OVER like";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(QueryValidatorTest, GoInvalid) {
    {
        // friend not exist.
        std::string query = "GO FROM \"1\" OVER friend";
        EXPECT_FALSE(checkResult(query));
    }
    {
        // manager not exist
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $^.manager.name,$^.person.age";
        EXPECT_FALSE(checkResult(query));
    }
    {
        // manager not exist
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.manager.name,$$.person.age";
        EXPECT_FALSE(checkResult(query));
    }
    {
        // column not exist
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.col OVER like";
        EXPECT_FALSE(checkResult(query));
    }
    {
        // invalid id type
        std::string query = "GO FROM \"1\" OVER like YIELD like.likeness AS id"
                            "| GO FROM $-.id OVER like";
        EXPECT_FALSE(checkResult(query));
    }
    {
        // multi inputs
        std::string query = "$var = GO FROM \"2\" OVER like;"
                            "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.id OVER like WHERE $var.id == \"\"";
        EXPECT_FALSE(checkResult(query));
    }
}

TEST_F(QueryValidatorTest, Limit) {
    // Syntax error
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD like._dst AS like | LIMIT -1, 3";
        auto status = validate(query);
        ASSERT_FALSE(status.ok()) << status.status();
    }
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD like._dst AS like | LIMIT 1, 3";
        std::vector<PlanNode::Kind> expected = {
                PK::kDataCollect, PK::kLimit, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(checkResult(query, expected));
    }
}

TEST_F(QueryValidatorTest, OrderBy) {
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
                            " | ORDER BY $-.age";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect, PK::kSort, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(checkResult(query, expected));
    }
    // not exist factor
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
                            " | ORDER BY $-.name";
        auto status = validate(query);
        ASSERT_FALSE(status.ok()) << status.status();
    }
}

TEST_F(QueryValidatorTest, OrderByAndLimt) {
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
                            " | ORDER BY $-.age | LIMIT 1";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect, PK::kLimit, PK::kSort, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(checkResult(query, expected));
    }
}

TEST_F(QueryValidatorTest, TestSetValidator) {
  // UNION ALL
  {
      std::string query =
          "GO FROM \"1\" OVER like YIELD like.start AS start UNION ALL GO FROM \"2\" "
          "OVER like YIELD like.start AS start";
      auto status = validate(query);
      ASSERT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kUnion,
          PK::kProject,
          PK::kProject,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kMultiOutputs,
          PK::kStart,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
  // UNION DISTINCT twice
  {
      std::string query = "GO FROM \"1\" OVER like YIELD like.start AS start UNION GO FROM \"2\" "
                          "OVER like YIELD like.start AS start UNION GO FROM \"3\" OVER like YIELD "
                          "like.start AS start";
      auto status = validate(query);
      ASSERT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kDedup,
          PK::kUnion,
          PK::kDedup,
          PK::kProject,
          PK::kUnion,
          PK::kGetNeighbors,
          PK::kProject,
          PK::kProject,
          PK::kMultiOutputs,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kStart,
          PK::kMultiOutputs,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
  // UNION DISTINCT
  {
      std::string query =
          "GO FROM \"1\" OVER like YIELD like.start AS start UNION DISTINCT GO FROM \"2\" "
          "OVER like YIELD like.start AS start";
      auto status = validate(query);
      EXPECT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kDedup,
          PK::kUnion,
          PK::kProject,
          PK::kProject,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kMultiOutputs,
          PK::kStart,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
  // INVALID UNION ALL
  {
      std::string query = "GO FROM \"1\" OVER like YIELD like.start AS start, $^.person.name AS "
                          "name UNION GO FROM \"2\" OVER like YIELD like.start AS start";
      auto status = validate(query);
      ASSERT_FALSE(status.ok()) << status.status();
  }
  // INTERSECT
  {
      std::string query = "GO FROM \"1\" OVER like YIELD like.start AS start INTERSECT GO FROM "
                          "\"2\" OVER like YIELD like.start AS start";
      auto status = validate(query);
      EXPECT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kIntersect,
          PK::kProject,
          PK::kProject,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kMultiOutputs,
          PK::kStart,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
  // MINUS
  {
      std::string query = "GO FROM \"1\" OVER like YIELD like.start AS start MINUS GO FROM "
                          "\"2\" OVER like YIELD like.start AS start";
      auto status = validate(query);
      EXPECT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kMinus,
          PK::kProject,
          PK::kProject,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kMultiOutputs,
          PK::kStart,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
}
}  // namespace graph
}  // namespace nebula
