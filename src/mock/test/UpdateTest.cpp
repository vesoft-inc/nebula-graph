/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "UpdateTestBase.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class UpdateTest : public UpdateTestBase {
protected:
    void SetUp() override {
        UpdateTestBase::SetUp();
    }

    void TearDown() override {
        UpdateTestBase::TearDown();
    }
};


TEST_F(UpdateTest, UpdateVertex) {
    // INSERT VERTEX course(name, credits), building(name) VALUES \"Math\":("Math", 3, "No5");
    {   // OnlySet
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX \"Math\" "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No6\" ";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilter
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX \"Math\" "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No7\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetYield
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX \"Math\" "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No8\" "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {
            {"Math", 6, "No8"},
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {   // SetFilterYield
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX \"Math\" "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No9\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {
            {"Math", 7, "No9"},
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {   // Insertable: vertex \"CS\" ("CS", 5) --> ("CS", 6, "No10")
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX \"CS\" "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No10\" "
                     "WHEN $^.course.name == \"CS\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {
            {"CS", 6, "No10"},
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }
}


TEST_F(UpdateTest, UpdateEdge) {
    // OnlySet, SetFilter, SetYield, SetFilterYield, Insertable
    {   // OnlySet
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"@0 OF select "
                     "SET grade = select.grade + 1, year = \"Monica\"0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilter
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"@0 OF select "
                     "SET grade = select.grade + 1, year = \"Monica\"0 "
                     "WHEN select.grade > 4 && $^.student.age > 15";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetYield
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"@0 OF select "
                     "SET grade = select.grade + 1, year = 2018 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {
            {"Monica", 8, 2018},
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {   // SetFilterYield
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"@0 OF select "
                     "SET grade = select.grade + 1, year = 2019 "
                     "WHEN select.grade > 4 && $^.student.age > 15 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {
            {"Monica", 9, 2019},
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {  // Insertable
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE \"Mike\" -> \"Math\"@0 OF select "
                     "SET grade = 3, year = 2019 "
                     "WHEN $^.student.age > 15 && $^.student.gender == \"male\" "
                     "YIELD select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {
            {3, 2019},
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }
}


TEST_F(UpdateTest, InvalidTest) {
    {   // update vertex: the item is TagName.PropName = Expression in SET clause
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX \"Math\" "
                     "SET credits = $^.course.credits + 1, name = \"No9\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {   // update edge: the item is PropName = Expression in SET clause
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"@0 OF select "
                     "SET select.grade = select.grade + 1, select.year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // the $$.TagName.PropName expressions are not allowed in any update sentence
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX \"Math\" "
                     "SET course.credits = $$.course.credits + 1 "
                     "WHEN $$.course.name == \"Math\" && $^.course.credits > $$.course.credits + 1 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $$.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // make sure the vertex must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX \"Math\"000000000000 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No9\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // make sure TagName and PropertyName must exist in all clauses
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX \"Math\" "
                     "SET nonexistentTag.credits = $^.course.credits + 1, building.name = \"No9\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.nonexistentProperty > 2 "
                     "YIELD $^.course.name AS Name, $^.nonexistentTag.nonexistentProperty";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // make sure the edge(src, dst) must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"000000000000@0 OF select "
                     "SET grade = select.grade + 1, year = 2019 "
                     "WHEN select.grade > 4 && $^.student.age > 15 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // make sure the edge(src, edge_tyep, dst) must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"@0 OF nonexistentEdgeTypeName "
                     "SET grade = select.grade + 1, year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // make sure the edge(src, edge_tyep, ranking<default is 0>, dst) must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\" OF select "
                     "SET grade = select.grade + 1, year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // make sure the edge(src, edge_tyep, ranking, dst) must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"@1000000000000 OF select "
                     "SET grade = select.grade + 1, year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // make sure EdgeName and PropertyName must exist in all clauses
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE \"Monica\" -> \"Math\"@0 OF select "
                     "SET nonexistentProperty = select.grade + 1, year = 2019 "
                     "WHEN nonexistentEdgeName.grade > 4 && $^.student.nonexistentProperty > 15 "
                     "YIELD $^.nonexistentTag.name AS Name, select.nonexistentProperty AS Grade";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}


}   // namespace graph
}   // namespace nebula
