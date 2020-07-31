/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GroupByValidator.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class GroupByValidatorTest : public ValidatorTestBase {
public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(GroupByValidatorTest, TestGroupBy) {
    {
        std::string query =
            "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
            "| GROUP BY $-.age YIELD COUNT($-.id)";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kAggregate,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query =
            "GO FROM \"NoExist\" OVER like YIELD like._dst AS id, $^.person.age AS age "
            "| GROUP BY $-.id YIELD $-.id AS id";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kAggregate,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(GroupByValidatorTest, InvalidTest) {
    {
        // use groupby without input
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY 1+1 YIELD COUNT(1), 1+1";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: Group `(1+1)` invalid");
    }
    {
        // src
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.age YIELD COUNT($var)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SyntaxError: syntax error near `)'");
    }
    {
        // use dst
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.age YIELD COUNT($$.player.name)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: TagName `player' not found");
    }
    {
        // group input noexist
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.start_year YIELD COUNT($-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `$-.start_year', not exist prop `start_year'");
    }
    {
        // group name noexist
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY noexist YIELD COUNT($-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: Group `noexist` invalid");
    }
    {
        // use sum(*)
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.id YIELD SUM(*)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `SUM(*)` invaild, * valid in count.");
    }
    {
        // use agg fun has more than two inputs
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.id YIELD COUNT($-.id, $-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SyntaxError: syntax error near `, $-.age'");
    }
    {
        // group col has agg fun
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.id, SUM($-.age) YIELD $-.id, SUM($-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: Use invalid group function `SUM`");
    }
    {
        // yield without group by
        std::string query = "GO FROM \"1\" OVER like YIELD $^.person.age AS age, "
                            "COUNT(like._dst) AS id ";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `COUNT(like._dst) AS id', not support "
                  "aggregate function in go sentence.");
    }
    // {
    //     // todo(jmq) not support $-.*
    //     std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age
    //                         "
    //                         "| GROUP BY $-.id YIELD  COUNT($-.*)";
    //     auto result = checkResult(query);
    //     EXPECT_EQ(std::string(result.message()), "SemanticError: Use invalid group function
    //     `SUM`");
    // }
    // {
    //     //  todo(jmq) not support $-.*
    //     std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age
    //                         "
    //                         "| GROUP BY $-.* YIELD  $-.*";
    //     auto result = checkResult(query);
    //     EXPECT_EQ(std::string(result.message()), "SemanticError: Use invalid group function
    //     `SUM`");
    // }
}

}  // namespace graph
}  // namespace nebula
