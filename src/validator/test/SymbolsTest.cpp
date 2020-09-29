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

TEST_F(SymbolsTest, Variables) {
    {
        std::string query = "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
                            "id | GO 2 STEPS FROM $-.id OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.ok());
        auto qctx = std::move(status).value();
        EXPECT_NE(qctx, nullptr);
        auto* symTable = qctx->symTable();

        {
            auto varName = "__Start_19";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kStart);
            EXPECT_TRUE(symTable->getDerivatives(varName).empty());
            EXPECT_TRUE(symTable->getDependencies(varName).empty());
        }
        {
            auto varName = "__GetNeighbors_0";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kGetNeighbors);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Project_1"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__UNAMED_VAR_0"}));
        }
        {
            auto varName = "__Project_1";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"id"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kProject);
            EXPECT_EQ(
                symTable->getDerivatives(varName),
                std::unordered_set<std::string>({"__Project_5", "__DataJoin_17", "__Project_3"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__GetNeighbors_0"}));
        }
        {
            auto varName = "__Project_3";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"_vid"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kProject);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Dedup_4"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Project_1"}));
        }
        {
            auto varName = "__Dedup_4";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"_vid"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kDedup);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>(
                          {"__GetNeighbors_7", "__DataJoin_10", "__GetNeighbors_14"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Project_3"}));
        }
        {
            auto varName = "__Project_5";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"id", "__UNAMED_COL_1"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kProject);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Dedup_6"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Project_1"}));
        }
        {
            auto varName = "__Dedup_6";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"id", "__UNAMED_COL_1"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kDedup);
            EXPECT_EQ(
                symTable->getDerivatives(varName),
                std::unordered_set<std::string>({"__DataJoin_10", "__Loop_13", "__DataJoin_16"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Project_5"}));
        }
        {
            auto varName = "__Start_2";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kStart);
            EXPECT_TRUE(symTable->getDerivatives(varName).empty());
            EXPECT_TRUE(symTable->getDependencies(varName).empty());
        }
        {
            auto varName = "__GetNeighbors_7";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kGetNeighbors);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Project_8"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Dedup_4"}));
        }
        {
            auto varName = "__Project_8";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"_vid"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kProject);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Dedup_4"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__GetNeighbors_7"}));
        }
        {
            auto varName = "__DataJoin_10";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames,
                      std::vector<std::string>({"id", "__UNAMED_COL_1", "_vid"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kDataJoin);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Project_11"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Dedup_4", "__Dedup_6"}));
        }
        {
            auto varName = "__Project_11";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"id", "__UNAMED_COL_1"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kProject);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Dedup_6"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__DataJoin_10"}));
        }
        {
            auto varName = "__GetNeighbors_7";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kGetNeighbors);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Project_8"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Dedup_4"}));
        }
        {
            auto varName = "__Project_15";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"__UNAMED_COL_0", "_vid"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kProject);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__DataJoin_16"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__GetNeighbors_14"}));
        }
        {
            auto varName = "__DataJoin_16";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames,
                      std::vector<std::string>({"__UNAMED_COL_0", "_vid", "id", "__UNAMED_COL_1"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kDataJoin);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__DataJoin_17"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Project_15", "__Dedup_6"}));
        }
        {
            auto varName = "__DataJoin_17";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames,
                      std::vector<std::string>(
                          {"__UNAMED_COL_0", "_vid", "id", "__UNAMED_COL_1", "like._dst"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kDataJoin);
            EXPECT_EQ(symTable->getDerivatives(varName),
                      std::unordered_set<std::string>({"__Project_18"}));
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__Project_1", "__DataJoin_16"}));
        }
        {
            auto varName = "__Project_18";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"like._dst"}));
            EXPECT_EQ(symTable->getOrigin(varName)->kind(), PlanNode::Kind::kProject);
            EXPECT_TRUE(symTable->getDerivatives(varName).empty());
            EXPECT_EQ(symTable->getDependencies(varName),
                      std::unordered_set<std::string>({"__DataJoin_17"}));
        }
    }
}
}  // namespace graph
}  // namespace nebula
