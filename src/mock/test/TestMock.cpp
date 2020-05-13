/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Status.h"
#include "common/network/NetworkUtils.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"
#include <gtest/gtest.h>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {
class MockServerTest : public TestBase {
public:
    void SetUp() override {
        TestBase::SetUp();
    };

    void TearDown() override {
        TestBase::TearDown();
    };
};

TEST_F(MockServerTest, TestMeta) {
    GraphSpaceID spaceId1 = 0;
    GraphSpaceID spaceId2 = 0;
    auto metaClient = gEnv->getMetaClient();
    // Test space
    {
        // Create space
        std::string spaceName1 = "TEST_SPACE1";
        meta::SpaceDesc spaceDesc1(spaceName1, 10, 1, "", "", 8);
        bool ifNotExists = true;
        auto status = metaClient->createSpace(spaceDesc1, ifNotExists).get();
        spaceId1 = status.value();

        std::string spaceName2 = "TEST_SPACE2";
        meta::SpaceDesc spaceDesc2(spaceName2, 100, 3, "", "", 8);
        status = metaClient->createSpace(spaceDesc2, ifNotExists).get();
        spaceId2 = status.value();

        // Get space
        auto getStatus = metaClient->getSpace(spaceName1).get();
        ASSERT_TRUE(getStatus.ok());
        ASSERT_EQ(spaceId1, getStatus.value().get_space_id());
        ASSERT_EQ(10, getStatus.value().get_properties().get_partition_num());
        ASSERT_EQ(1, getStatus.value().get_properties().get_replica_factor());
        ASSERT_EQ(8, getStatus.value().get_properties().get_vid_size());

        getStatus = metaClient->getSpace(spaceName2).get();
        ASSERT_TRUE(getStatus.ok());
        ASSERT_EQ(spaceId2, getStatus.value().get_space_id());
        ASSERT_EQ(100, getStatus.value().get_properties().get_partition_num());
        ASSERT_EQ(3, getStatus.value().get_properties().get_replica_factor());
        ASSERT_EQ(8, getStatus.value().get_properties().get_vid_size());

        // List spaces
        auto listStatus = metaClient->listSpaces().get();
        ASSERT_TRUE(listStatus.ok());
        auto spaces = listStatus.value();
        ASSERT_EQ(2, spaces.size());

        std::vector<meta::SpaceIdName> expected;
        expected.emplace_back(spaceId2, spaceName2);
        expected.emplace_back(spaceId1, spaceName1);
        ASSERT_EQ(expected, spaces);
    }

    // Test tag
    {
        // Create tag
        for (auto i = 0u; i < 10; i++) {
            meta::cpp2::Schema tagSchema;
            meta::cpp2::ColumnDef col;
            col.set_name(folly::stringPrintf("col_%d", i));
            col.set_type(meta::cpp2::PropertyType::STRING);
            col.set_default_value(nebula::Value("NULL"));
            std::vector<meta::cpp2::ColumnDef> cols;
            cols.emplace_back(col);
            tagSchema.set_columns(std::move(cols));
            auto status = metaClient->createTagSchema(spaceId1,
                    folly::stringPrintf("tag_%d", i), tagSchema, false).get();
            ASSERT_TRUE(status.ok());
        }

        // Get tag
        for (auto i = 0u; i < 10; i++) {
            auto status = metaClient->getTagSchema(spaceId1,
                    folly::stringPrintf("tag_%d", i)).get();
            ASSERT_TRUE(status.ok());
            auto schema = status.value();
            ASSERT_EQ(1, schema.get_columns().size());
            ASSERT_EQ(folly::stringPrintf("col_%d", i), schema.get_columns()[0].get_name());
            ASSERT_EQ(meta::cpp2::PropertyType::STRING, schema.get_columns()[0].get_type());
            ASSERT_EQ("NULL", schema.get_columns()[0].get_default_value()->getStr());
        }

        // List tags
        auto listStatus = metaClient->listTagSchemas(spaceId1).get();
        ASSERT_TRUE(listStatus.ok());
        ASSERT_EQ(10, listStatus.value().size());

        // Drop tag
        for (auto i = 5u; i < 10; i++) {
            auto status = metaClient->dropTagSchema(spaceId1,
                    folly::stringPrintf("tag_%d", i), true).get();
            ASSERT_TRUE(status.ok());
        }

        // List tags
        listStatus = metaClient->listTagSchemas(spaceId1).get();
        ASSERT_TRUE(listStatus.ok());
        ASSERT_EQ(5, listStatus.value().size());
    }

    // Test edge
    {
        // Create edge
        for (auto i = 0u; i < 10; i++) {
            meta::cpp2::Schema edgeSchema;
            meta::cpp2::ColumnDef col;
            col.set_name(folly::stringPrintf("col_%d", i));
            col.set_type(meta::cpp2::PropertyType::STRING);
            col.set_default_value(nebula::Value("NULL"));
            std::vector<meta::cpp2::ColumnDef> cols;
            cols.emplace_back(col);
            edgeSchema.set_columns(std::move(cols));
            auto status = metaClient->createEdgeSchema(spaceId1,
                    folly::stringPrintf("edge_%d", i), edgeSchema, false).get();
            ASSERT_TRUE(status.ok());
        }

        // Get edge
        for (auto i = 0u; i < 10; i++) {
            auto status = metaClient->getEdgeSchema(spaceId1,
                    folly::stringPrintf("edge_%d", i)).get();
            ASSERT_TRUE(status.ok());
            auto schema = status.value();
            ASSERT_EQ(1, schema.get_columns().size());
            ASSERT_EQ(folly::stringPrintf("col_%d", i), schema.get_columns()[0].get_name());
            ASSERT_EQ(meta::cpp2::PropertyType::STRING, schema.get_columns()[0].get_type());
            ASSERT_EQ("NULL", schema.get_columns()[0].get_default_value()->getStr());
        }

        // List edges
        auto listStatus = metaClient->listEdgeSchemas(spaceId1).get();
        ASSERT_TRUE(listStatus.ok());
        ASSERT_EQ(10, listStatus.value().size());

        // Drop edge
        for (auto i = 5u; i < 10; i++) {
            auto status = metaClient->dropEdgeSchema(spaceId1,
                    folly::stringPrintf("edge_%d", i), true).get();
            ASSERT_TRUE(status.ok());
        }

        // List edges
        listStatus = metaClient->listEdgeSchemas(spaceId1).get();
        ASSERT_TRUE(listStatus.ok());
        ASSERT_EQ(5, listStatus.value().size());
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
}

TEST_F(MockServerTest, DISABLED_TestStorage) {
    auto metaClient = gEnv->getMetaClient();
    auto storageClient = gEnv->getStorageClient();
    // create tag person
    {
        meta::cpp2::Schema tagSchema;
        std::vector<meta::cpp2::ColumnDef> cols;

        meta::cpp2::ColumnDef col;
        col.set_name("name");
        col.set_type(meta::cpp2::PropertyType::STRING);
        col.set_default_value(nebula::Value("NULL"));

        cols.emplace_back(col);
        col.set_name("age");
        col.set_type(meta::cpp2::PropertyType::INT8);
        col.set_default_value(nebula::Value(18));
        cols.emplace_back(std::move(col));

        tagSchema.set_columns(std::move(cols));
        auto status = metaClient->createTagSchema(1, "person", tagSchema, false).get();
        ASSERT_TRUE(status.ok());

        sleep(FLAGS_heartbeat_interval_secs + 1);
    }

    // add vertex
    {
        auto ret = metaClient->getTagIDByNameFromCache(1, "person");
        ASSERT_TRUE(ret.ok());
        auto tagId = ret.value();
        GraphSpaceID space = 1;
        std::vector<Value> props;
        props.emplace_back("laura");
        storage::cpp2::NewTag tag;
        tag.set_tag_id(tagId);
        tag.set_props(std::move(props));
        std::vector<storage::cpp2::NewTag> tags;
        tags.emplace_back(tag);

        storage::cpp2::NewVertex vertex;
        vertex.set_id("laura");
        vertex.set_tags(std::move(tags));
        std::vector<storage::cpp2::NewVertex> vertices;
        vertices.emplace_back(std::move(vertex));

        std::unordered_map<TagID, std::vector<std::string>> propNames;
        propNames[tagId] = {"name"};
        auto resp = storageClient->addVertices(space, vertices, propNames, false).get();
        ASSERT_TRUE(resp.succeeded());
    }

    // Get person.name
    {
        std::vector<Row> vids;
        Row row;
        row.columns.emplace_back("laura");
        vids.emplace_back(std::move(row));
        storage::cpp2::PropExp prop;
        prop.set_prop("person.name");
        prop.set_alias("p_name");
        auto getResp = storageClient->getProps(1, {"_vid"}, vids, {prop}).get();
        ASSERT_TRUE(getResp.succeeded());
        auto response = getResp.responses();
        ASSERT_EQ(1, response.size());
        ASSERT_NE(nullptr, response[0].get_props());
        ASSERT_EQ(2, response[0].get_props()->colNames.size());
        ASSERT_EQ("_vid", response[0].get_props()->colNames[0]);
        ASSERT_EQ("person:name_p_name", response[0].get_props()->colNames[1]);
        ASSERT_EQ(1, response[0].get_props()->rows.size());
        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[0]);
        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[1]);
    }

    // Get person.*
    {
        std::vector<Row> vids;
        Row row;
        row.columns.emplace_back("laura");
        vids.emplace_back(std::move(row));
        storage::cpp2::PropExp prop;
        prop.set_prop("person.*");
        prop.set_alias("");
        auto getResp = storageClient->getProps(1, {"_vid"}, vids, {prop}).get();
        ASSERT_TRUE(getResp.succeeded());
        auto response = getResp.responses();
        ASSERT_EQ(1, response.size());
        ASSERT_NE(nullptr, response[0].get_props());
        ASSERT_EQ(3, response[0].get_props()->colNames.size());
        ASSERT_EQ("_vid", response[0].get_props()->colNames[0]);
        ASSERT_EQ(1, response[0].get_props()->rows.size());
        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[0]);
        if ("person:age_age" == response[0].get_props()->colNames[1]) {
            ASSERT_EQ("person:name_name", response[0].get_props()->colNames[2]);
            ASSERT_EQ(Value(18), response[0].get_props()->rows[0].columns[1]);
            ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[2]);
        } else if ("person:name_name" == response[0].get_props()->colNames[1]) {
            ASSERT_EQ("person:age_age", response[0].get_props()->colNames[2]);
            ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[1]);
            ASSERT_EQ(Value(18), response[0].get_props()->rows[0].columns[2]);
        } else {
            ASSERT_TRUE(false);
        }
    }

    // Get all
    {
        std::vector<Row> vids;
        Row row;
        row.columns.emplace_back("laura");
        vids.emplace_back(std::move(row));
        auto getResp = storageClient->getProps(1, {"_vid"}, vids, {}).get();
        ASSERT_TRUE(getResp.succeeded());
        auto response = getResp.responses();
        ASSERT_EQ(1, response.size());
        ASSERT_NE(nullptr, response[0].get_props());
        ASSERT_EQ(3, response[0].get_props()->colNames.size());
        ASSERT_EQ("_vid", response[0].get_props()->colNames[0]);
        ASSERT_EQ(1, response[0].get_props()->rows.size());
        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[0]);
        if ("person:age_age" == response[0].get_props()->colNames[1]) {
            ASSERT_EQ("person:name_name", response[0].get_props()->colNames[2]);
            ASSERT_EQ(Value(18), response[0].get_props()->rows[0].columns[1]);
            ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[2]);
        } else if ("person:name_name" == response[0].get_props()->colNames[1]) {
            ASSERT_EQ("person:age_age", response[0].get_props()->colNames[2]);
            ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[1]);
            ASSERT_EQ(Value(18), response[0].get_props()->rows[0].columns[2]);
        } else {
            ASSERT_TRUE(false);
        }
    }
}


TEST_F(MockServerTest, TestStorageGetEdges) {
    auto metaClient = gEnv->getMetaClient();
    auto storageClient = gEnv->getStorageClient();
    // Create edge classmate
    {
        meta::cpp2::Schema edgeSchema;
        std::vector<meta::cpp2::ColumnDef> cols;

        meta::cpp2::ColumnDef col;
        col.set_name("start");
        col.set_type(meta::cpp2::PropertyType::INT16);
        col.set_default_value(nebula::Value(2010));

        cols.emplace_back(col);
        col.set_name("end");
        col.set_type(meta::cpp2::PropertyType::INT16);
        col.set_default_value(nebula::Value(2014));
        cols.emplace_back(std::move(col));

        edgeSchema.set_columns(std::move(cols));
        auto status = metaClient->createEdgeSchema(1, "classmate", edgeSchema, false).get();
        ASSERT_TRUE(status.ok());

        sleep(FLAGS_heartbeat_interval_secs + 1);
    }

    auto ret = metaClient->getEdgeTypeByNameFromCache(1, "classmate");
    ASSERT_TRUE(ret.ok());
    auto edgeType = ret.value();
    GraphSpaceID space = 1;
    // Add edge
    {
        std::vector<Value> props;
        props.emplace_back(2012);
        storage::cpp2::NewEdge edge;
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src("laura");
        edgeKey.set_edge_type(edgeType);
        edgeKey.set_ranking(1);
        edgeKey.set_dst("laura");
        edge.set_key(edgeKey);
        edge.set_props(std::move(props));
        std::vector<storage::cpp2::NewEdge> edges;
        edges.emplace_back(std::move(edge));

        std::vector<std::string> propNames = {"start"};
        auto resp = storageClient->addEdges(space, edges, propNames, false).get();
        ASSERT_TRUE(resp.succeeded());
    }

    // Get edge.start
    {
        std::vector<Row> edgeKeys;
        Row row;
        row.columns.emplace_back("laura");
        row.columns.emplace_back(edgeType);
        row.columns.emplace_back(1);
        row.columns.emplace_back("laura");
        edgeKeys.emplace_back(std::move(row));
        storage::cpp2::PropExp prop;
        prop.set_prop("classmate.start");
        prop.set_alias("start");
        auto getResp = storageClient->getProps(space,
                {"_src", "_type", "_rank", "_dst"}, edgeKeys, {prop}).get();
        ASSERT_TRUE(getResp.succeeded());
        auto response = getResp.responses();
        ASSERT_EQ(1, response.size());
        ASSERT_NE(nullptr, response[0].get_props());
        ASSERT_EQ(5, response[0].get_props()->colNames.size());
        ASSERT_EQ("_src", response[0].get_props()->colNames[0]);
        ASSERT_EQ("_type", response[0].get_props()->colNames[1]);
        ASSERT_EQ("_rank", response[0].get_props()->colNames[2]);
        ASSERT_EQ("_dst", response[0].get_props()->colNames[3]);
        ASSERT_EQ("classmate:start_start", response[0].get_props()->colNames[4]);
        ASSERT_EQ(1, response[0].get_props()->rows.size());
        ASSERT_EQ(5, response[0].get_props()->rows[0].columns.size());
        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[0]);
        ASSERT_EQ(Value(edgeType), response[0].get_props()->rows[0].columns[1]);
        ASSERT_EQ(Value(1), response[0].get_props()->rows[0].columns[2]);
        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[3]);
        ASSERT_EQ(Value(2012), response[0].get_props()->rows[0].columns[4]);
    }

    // Get classmate.*
    {
        std::vector<Row> edgeKeys;
        Row row;
        row.columns.emplace_back("laura");
        row.columns.emplace_back(edgeType);
        row.columns.emplace_back(1);
        row.columns.emplace_back("laura");
        edgeKeys.emplace_back(std::move(row));
        storage::cpp2::PropExp prop;
        prop.set_prop("classmate.*");
        auto getResp = storageClient->getProps(space,
                {"_src", "_type", "_rank", "_dst"}, edgeKeys, {}).get();
        ASSERT_TRUE(getResp.succeeded());
        auto response = getResp.responses();
        ASSERT_EQ(1, response.size());
        ASSERT_NE(nullptr, response[0].get_props());
        ASSERT_EQ(6, response[0].get_props()->colNames.size());
        ASSERT_EQ(1, response[0].get_props()->rows.size());
        ASSERT_EQ(6, response[0].get_props()->rows[0].columns.size());

        ASSERT_EQ("_src", response[0].get_props()->colNames[0]);
        ASSERT_EQ("_type", response[0].get_props()->colNames[1]);
        ASSERT_EQ("_rank", response[0].get_props()->colNames[2]);
        ASSERT_EQ("_dst", response[0].get_props()->colNames[3]);

        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[0]);
        ASSERT_EQ(Value(edgeType), response[0].get_props()->rows[0].columns[1]);
        ASSERT_EQ(Value(1), response[0].get_props()->rows[0].columns[2]);
        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[3]);

        if ("classmate:start_start" == response[0].get_props()->colNames[4]) {
            ASSERT_EQ("classmate:end_end", response[0].get_props()->colNames[5]);
            ASSERT_EQ(Value(2012), response[0].get_props()->rows[0].columns[4]);
            ASSERT_EQ(Value(2014), response[0].get_props()->rows[0].columns[5]);
        } else if ("classmate:end_end" == response[0].get_props()->colNames[4]) {
            ASSERT_EQ("classmate:start_start", response[0].get_props()->colNames[5]);
            ASSERT_EQ(Value(2014), response[0].get_props()->rows[0].columns[4]);
            ASSERT_EQ(Value(2012), response[0].get_props()->rows[0].columns[5]);
        } else {
            ASSERT_TRUE(false);
        }
    }

    // Get all
    {
        std::vector<Row> edgeKeys;
        Row row;
        row.columns.emplace_back("laura");
        row.columns.emplace_back(edgeType);
        row.columns.emplace_back(1);
        row.columns.emplace_back("laura");
        edgeKeys.emplace_back(std::move(row));
        auto getResp = storageClient->getProps(space,
                {"_src", "_type", "_rank", "_dst"}, edgeKeys, {}).get();
        ASSERT_TRUE(getResp.succeeded());
        auto response = getResp.responses();
        ASSERT_EQ(1, response.size());
        ASSERT_NE(nullptr, response[0].get_props());
        ASSERT_EQ(6, response[0].get_props()->colNames.size());
        ASSERT_EQ(1, response[0].get_props()->rows.size());
        ASSERT_EQ(6, response[0].get_props()->rows[0].columns.size());

        ASSERT_EQ("_src", response[0].get_props()->colNames[0]);
        ASSERT_EQ("_type", response[0].get_props()->colNames[1]);
        ASSERT_EQ("_rank", response[0].get_props()->colNames[2]);
        ASSERT_EQ("_dst", response[0].get_props()->colNames[3]);

        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[0]);
        ASSERT_EQ(Value(edgeType), response[0].get_props()->rows[0].columns[1]);
        ASSERT_EQ(Value(1), response[0].get_props()->rows[0].columns[2]);
        ASSERT_EQ(Value("laura"), response[0].get_props()->rows[0].columns[3]);

        if ("classmate:start_start" == response[0].get_props()->colNames[4]) {
            ASSERT_EQ("classmate:end_end", response[0].get_props()->colNames[5]);
            ASSERT_EQ(Value(2012), response[0].get_props()->rows[0].columns[4]);
            ASSERT_EQ(Value(2014), response[0].get_props()->rows[0].columns[5]);
        } else if ("classmate:end_end" == response[0].get_props()->colNames[4]) {
            ASSERT_EQ("classmate:start_start", response[0].get_props()->colNames[5]);
            ASSERT_EQ(Value(2014), response[0].get_props()->rows[0].columns[4]);
            ASSERT_EQ(Value(2012), response[0].get_props()->rows[0].columns[5]);
        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(MockServerTest, TestStorageGetNeighbors) {
    auto metaClient = gEnv->getMetaClient();
    auto storageClient = gEnv->getStorageClient();
    // GetNeighbors
    {
        auto ret = metaClient->getEdgeTypeByNameFromCache(1, "classmate");
        ASSERT_TRUE(ret.ok());
        auto edgeType = ret.value();

        GraphSpaceID space = 1;
        std::vector<std::string> colNames = {"_vid"};
        std::vector<Row> vertices;
        Row row;
        row.columns = {Value("laura")};
        vertices.emplace_back(row);
        std::vector<EdgeType> edgeTypes;
        edgeTypes.emplace_back(edgeType);
        storage::cpp2::EdgeDirection edgeDirection = storage::cpp2::EdgeDirection::BOTH;
        std::vector<storage::cpp2::StatProp> statProps;
        storage::cpp2::PropExp propExp;
        propExp.prop = "person.name";
        propExp.alias = "name";
        std::vector<storage::cpp2::PropExp> vertexProps = {propExp};

        std::vector<storage::cpp2::PropExp> edgeProps;
        propExp.prop = "classmate._src";
        propExp.alias = "src";
        edgeProps.emplace_back(propExp);
        propExp.prop = "classmate._type";
        propExp.alias = "type";
        edgeProps.emplace_back(propExp);
        propExp.prop = "classmate._rank";
        propExp.alias = "ranking";
        edgeProps.emplace_back(propExp);
        propExp.prop = "classmate._dst";
        propExp.alias = "dst";
        edgeProps.emplace_back(propExp);
        propExp.prop = "classmate.start";
        propExp.alias = "start";
        edgeProps.emplace_back(propExp);

        auto resp = storageClient->getNeighbors(space,
                                                colNames,
                                                vertices,
                                                edgeTypes,
                                                edgeDirection,
                                                &statProps,
                                                &vertexProps,
                                                &edgeProps).get();
        ASSERT_TRUE(resp.succeeded());
        auto response = resp.responses();
        ASSERT_EQ(1, response.size());
        ASSERT_NE(nullptr, response[0].get_vertices());
        ASSERT_EQ(4, response[0].get_vertices()->colNames.size());
        ASSERT_EQ("_vid", response[0].get_vertices()->colNames[0]);
        ASSERT_EQ("_stat:", response[0].get_vertices()->colNames[1]);
        ASSERT_EQ("_tag:person:name_name", response[0].get_vertices()->colNames[2]);
        ASSERT_EQ("_edge:classmate:_src_src:_type_type:_rank_ranking:_dst_dst:start_start",
                response[0].get_vertices()->colNames[3]);

        ASSERT_EQ(1, response[0].get_vertices()->rows.size());
        ASSERT_EQ(4, response[0].get_vertices()->rows[0].columns.size());
        auto &resultRow = response[0].get_vertices()->rows[0];
        // vid
        ASSERT_EQ(Value("laura"), resultRow.columns[0]);
        // stat
        ASSERT_EQ(Value(), resultRow.columns[1]);

        // vid prop
        auto &verticesValue = resultRow.columns[2].getList().values;
        ASSERT_EQ(Value("laura"), verticesValue[0]);

        // edge prop
        auto &edgeValue = resultRow.columns[3].getList().values[0].getList().values;
        ASSERT_EQ(5, edgeValue.size());
        ASSERT_EQ(Value("laura"), edgeValue[0]);
        ASSERT_EQ(Value(edgeType), edgeValue[1]);
        ASSERT_EQ(Value(1), edgeValue[2]);
        ASSERT_EQ(Value("laura"), edgeValue[3]);
        ASSERT_EQ(Value(2012), edgeValue[4]);
    }
}

}   // namespace graph
}   // namespace nebula

