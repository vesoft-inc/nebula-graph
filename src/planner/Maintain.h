/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MAINTAIN_H_
#define PLANNER_MAINTAIN_H_

<<<<<<< HEAD
#include "Query.h"
=======
#include "planner/Query.h"
>>>>>>> Support DML,DDL to use inputNode
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
// which would make them in a single and big execution plan
<<<<<<< HEAD
class SchemaNode : public SingleInputNode {
public:
    GraphSpaceID space() const {
        return space_;
    }

protected:
    SchemaNode(ExecutionPlan* plan, Kind kind, PlanNode* input, const GraphSpaceID space)
        : SingleInputNode(plan, kind, input), space_(space) {}

protected:
    GraphSpaceID        space_;
};

class CreateSchemaNode : public SchemaNode {
=======
class CreateSchemaNode : public SingleInputNode {
>>>>>>> Support DML,DDL to use inputNode
protected:
    CreateSchemaNode(ExecutionPlan* plan,
                     PlanNode* input,
                     Kind kind,
<<<<<<< HEAD
                     PlanNode* input,
                     GraphSpaceID space,
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SchemaNode(plan, kind, input, space)
=======
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SingleInputNode(plan, kind, input)
>>>>>>> Support DML,DDL to use inputNode
        , name_(std::move(name))
        , schema_(std::move(schema))
        , ifNotExists_(ifNotExists) {}

public:
    const std::string& getName() const {
        return name_;
    }

    const meta::cpp2::Schema& getSchema() const {
        return schema_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

protected:
    std::string            name_;
    meta::cpp2::Schema     schema_;
    bool                   ifNotExists_;
};

class CreateTag final : public CreateSchemaNode {
public:
    static CreateTag* make(ExecutionPlan* plan,
                           PlanNode* input,
<<<<<<< HEAD
                           GraphSpaceID space,
=======
>>>>>>> Support DML,DDL to use inputNode
                           std::string tagName,
                           meta::cpp2::Schema schema,
                           bool ifNotExists) {
    return new CreateTag(plan,
                         input,
<<<<<<< HEAD
                         space,
=======
>>>>>>> Support DML,DDL to use inputNode
                         std::move(tagName),
                         std::move(schema),
                         ifNotExists);
    }

    std::string explain() const override {
        return "CreateTag";
    }

private:
    CreateTag(ExecutionPlan* plan,
              PlanNode* input,
<<<<<<< HEAD
              GraphSpaceID space,
              std::string tagName,
              meta::cpp2::Schema schema,
              bool ifNotExists)
    : CreateSchemaNode(plan,
                       Kind::kCreateTag,
                       input,
                       space,
                       std::move(tagName),
                       std::move(schema),
                       ifNotExists) {
    }
=======
              std::string tagName,
              meta::cpp2::Schema schema,
              bool ifNotExists)
        : CreateSchemaNode(plan,
                           input,
                           Kind::kCreateTag,
                           std::move(tagName),
                           std::move(schema),
                           ifNotExists) {
        }
>>>>>>> Support DML,DDL to use inputNode
};

class CreateEdge final : public CreateSchemaNode {
public:
    static CreateEdge* make(ExecutionPlan* plan,
                            PlanNode* input,
<<<<<<< HEAD
                            GraphSpaceID space,
=======
>>>>>>> Support DML,DDL to use inputNode
                            std::string edgeName,
                            meta::cpp2::Schema schema,
                            bool ifNotExists) {
    return new CreateEdge(plan,
                          input,
<<<<<<< HEAD
                          space,
=======
>>>>>>> Support DML,DDL to use inputNode
                          std::move(edgeName),
                          std::move(schema),
                          ifNotExists);
    }

    std::string explain() const override {
        return "CreateEdge";
    }

private:
    CreateEdge(ExecutionPlan* plan,
               PlanNode* input,
<<<<<<< HEAD
               GraphSpaceID space,
=======
>>>>>>> Support DML,DDL to use inputNode
               std::string edgeName,
               meta::cpp2::Schema schema,
               bool ifNotExists)
        : CreateSchemaNode(plan,
                           input,
                           Kind::kCreateEdge,
<<<<<<< HEAD
                           input,
                           space,
=======
>>>>>>> Support DML,DDL to use inputNode
                           std::move(edgeName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class AlterTag final : public SingleInputNode {
public:
    std::string explain() const override {
        return "AlterTag";
    }
};

class AlterEdge final : public SingleInputNode {
public:
    std::string explain() const override {
        return "AlterEdge";
    }
};

class DescSchema : public SingleInputNode {
protected:
    DescSchema(ExecutionPlan* plan,
               PlanNode* input,
               Kind kind,
<<<<<<< HEAD
               PlanNode* input,
               GraphSpaceID space,
               std::string name)
        : SingleInputNode(plan, kind, input)
        , space_(space)
        , name_(std::move(name)) {}
=======
               std::string name)
        : SingleInputNode(plan, kind, input)
        , name_(std::move(name)) {
    }
>>>>>>> Support DML,DDL to use inputNode

public:
    const std::string& getName() const {
        return name_;
    }

protected:
    std::string            name_;
};

class DescTag final : public DescSchema {
public:
    static DescTag* make(ExecutionPlan* plan,
                         PlanNode* input,
<<<<<<< HEAD
                         GraphSpaceID space,
                         std::string tagName) {
        return new DescTag(plan, input, space, std::move(tagName));
=======
                         std::string tagName) {
        return new DescTag(plan, input, std::move(tagName));
>>>>>>> Support DML,DDL to use inputNode
    }

    std::string explain() const override {
        return "DescTag";
    }

private:
    DescTag(ExecutionPlan* plan,
            PlanNode* input,
<<<<<<< HEAD
            GraphSpaceID space,
            std::string tagName)
    : DescSchema(plan, Kind::kDescTag, input, space, std::move(tagName)) {
=======
            std::string tagName)
        : DescSchema(plan, input, Kind::kDescTag, std::move(tagName)) {
>>>>>>> Support DML,DDL to use inputNode
    }
};

class DescEdge final : public DescSchema {
public:
    static DescEdge* make(ExecutionPlan* plan,
                          PlanNode* input,
<<<<<<< HEAD
                          GraphSpaceID space,
                          std::string edgeName) {
        return new DescEdge(plan, input, space, std::move(edgeName));
=======
                          std::string edgeName) {
        return new DescEdge(plan, input, std::move(edgeName));
>>>>>>> Support DML,DDL to use inputNode
    }

    std::string explain() const override {
        return "DescEdge";
    }

private:
    DescEdge(ExecutionPlan* plan,
             PlanNode* input,
<<<<<<< HEAD
             GraphSpaceID space,
             std::string edgeName)
    : DescSchema(plan, Kind::kDescEdge, input, space, std::move(edgeName)) {
=======
             std::string edgeName)
        : DescSchema(plan, input, Kind::kDescEdge, std::move(edgeName)) {
>>>>>>> Support DML,DDL to use inputNode
    }
};

class DropTag final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DropTag";
    }
};

class DropEdge final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DropEdge";
    }
};

class CreateTagIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "CreateTagIndex";
    }
};

class CreateEdgeIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "CreateEdgeIndex";
    }
};

class DescribeTagIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DescribeTagIndex";
    }
};

class DescribeEdgeIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DescribeEdgeIndex";
    }
};

class DropTagIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DropTagIndex";
    }
};

class DropEdgeIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "DropEdgeIndex";
    }
};

class BuildTagIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "BuildTagIndex";
    }
};

class BuildEdgeIndex final : public SingleInputNode {
public:
    std::string explain() const override {
        return "BuildEdgeIndex";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MAINTAIN_H_
