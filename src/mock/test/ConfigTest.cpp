/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Status.h"
#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"
#include <gtest/gtest.h>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {
class ConfigTest : public TestBase {
public:
    void SetUp() override {
        TestBase::SetUp();
    };

    void TearDown() override {
        TestBase::TearDown();
    };

    static void SetUpTestCase() {
        client_ = gEnv->getGraphClient();
        ASSERT_NE(nullptr, client_);
    }

    static void TearDownTestCase() {
        client_.reset();
    }

protected:
    static std::unique_ptr<GraphClient>     client_;
};

std::unique_ptr<GraphClient> ConfigTest::client_{nullptr};

meta::cpp2::ConfigItem initConfigItem(meta::cpp2::ConfigModule module,
                                      std::string name,
                                      meta::cpp2::ConfigMode mode,
                                      Value value) {
    meta::cpp2::ConfigItem configItem;
    configItem.set_module(module);
    configItem.set_name(name);
    configItem.set_mode(mode);
    configItem.set_value(value);
    return configItem;
}

std::vector<meta::cpp2::ConfigItem> mockRegisterGflags() {
    using ConfigModule = meta::cpp2::ConfigModule;
    using ConfigMode = meta::cpp2::ConfigMode;
    using ConfigItem = meta::cpp2::ConfigItem;
    std::vector<ConfigItem> configItems;
    configItems.emplace_back(initConfigItem(ConfigModule::STORAGE,
            "k0", ConfigMode::IMMUTABLE, Value(10)));
    configItems.emplace_back(initConfigItem(ConfigModule::STORAGE,
            "k1", ConfigMode::MUTABLE, Value(10)));
    configItems.emplace_back(initConfigItem(ConfigModule::STORAGE,
            "k2", ConfigMode::MUTABLE, Value(false)));
    configItems.emplace_back(initConfigItem(ConfigModule::STORAGE,
            "k3", ConfigMode::MUTABLE, Value("nebula")));
    configItems.emplace_back(initConfigItem(ConfigModule::GRAPH,
            "k1", ConfigMode::MUTABLE, Value(1.0)));
    configItems.emplace_back(initConfigItem(ConfigModule::GRAPH,
            "k2", ConfigMode::MUTABLE, Value("nebula")));
    configItems.emplace_back(initConfigItem(ConfigModule::GRAPH,
            "k3", ConfigMode::MUTABLE, Value("nebula")));
    Map map;
    map.kvs["disable_auto_compactions"] = "false";
    map.kvs["write_buffer_size"] = "1048576";
    configItems.emplace_back(initConfigItem(ConfigModule::STORAGE,
            "k4", ConfigMode::MUTABLE, Value(map)));
    return configItems;
}

TEST_F(ConfigTest, ConfigTest) {
    auto configItems = mockRegisterGflags();
    auto metaClient = gEnv->getMetaClient();
    auto ret = metaClient->regConfig(std::move(configItems)).get();
    ASSERT_TRUE(ret.ok()) << ret.status();

    // show all configs
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CONFIGS";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"module", "name", "type", "mode", "value"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        Map map;
        map.kvs["disable_auto_compactions"] = "false";
        map.kvs["write_buffer_size"] = "1048576";
        std::vector<std::vector<Value>> expected = {
            {"GRAPH", "k1", "FLOAT", "MUTABLE", 1.0},
            {"GRAPH", "k2", "STRING", "MUTABLE", "nebula"},
            {"GRAPH", "k3", "STRING", "MUTABLE", "nebula"},
            {"STORAGE", "k0", "INT", "IMMUTABLE", 10},
            {"STORAGE", "k1", "INT", "MUTABLE", 10},
            {"STORAGE", "k2", "BOOL", "MUTABLE", false},
            {"STORAGE", "k3", "STRING", "MUTABLE", "nebula"},
            {"STORAGE", "k4", "MAP", "MUTABLE", Value(map)}
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }

    // show all storage configs
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CONFIGS graph";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"module", "name", "type", "mode", "value"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> expected = {
                {"GRAPH", "k1", "FLOAT", "MUTABLE", 1.0},
                {"GRAPH", "k2", "STRING", "MUTABLE", "nebula"},
                {"GRAPH", "k3", "STRING", "MUTABLE", "nebula"}
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }

    // set/get without declaration
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:notRegistered=123";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
        query = "GET CONFIGS storage:notRegistered";
        code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // update immutable config will fail, read-only
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:k0=123";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS storage:k0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"module", "name", "type", "mode", "value"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<Value> expected = {"STORAGE", "k0", "INT", "IMMUTABLE", 10};
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    // set and get config after declaration
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:k1=123";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS storage:k1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {"STORAGE", "k1", "INT", "MUTABLE", 123};
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS graph:k1=3.14";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS graph:k1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {"GRAPH", "k1", "FLOAT", "MUTABLE", 3.140000};
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:k2=True";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS storage:k2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {"STORAGE", "k2", "BOOL", "MUTABLE", true};
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS graph:k2=abc";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS graph:k2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<Value> expected = {"GRAPH", "k2", "STRING", "MUTABLE", "abc"};
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    // list configs in a specified module
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CONFIGS storage";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        ASSERT_EQ(5, (*resp.get_data())[0].rowSize());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CONFIGS graph";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // set and get a config of all module
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS k3=vesoft";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS k3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::vector<Value>> expected =  {
                {"GRAPH", "k3", "STRING", "MUTABLE", "vesoft"},
                {"STORAGE", "k3", "STRING", "MUTABLE", "vesoft"},
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS graph:k3=abc";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "UPDATE CONFIGS storage:k3=cde";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS k3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::vector<Value>> expected = {
                {"GRAPH", "k3", "STRING", "MUTABLE", "abc"},
                {"STORAGE", "k3", "STRING", "MUTABLE", "cde"},
        };
        ASSERT_TRUE(verifyValues(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:k4 = {"
                            "disable_auto_compactions = true,"
                            "level0_file_num_compaction_trigger=4"
                            "}";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}
}   // namespace graph
}   // namespace nebula
