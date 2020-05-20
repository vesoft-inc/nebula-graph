/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MAINTAIN_H_
#define PLANNER_MAINTAIN_H_

#include "PlanNode.h"
#include "interface/gen-cpp2/meta_types.h"
#include "clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
// TODO: All DDLs, DMLs and DQLs could be used in a single query
// which would make them in a single and big execution plan
class SchemaNode : public PlanNode {
public:
    GraphSpaceID space() const {
        return space_;
    }

protected:
    SchemaNode(ExecutionPlan* plan, Kind kind, const GraphSpaceID space)
        : PlanNode(plan, kind), space_(space) {}

protected:
    GraphSpaceID        space_;
};

class CreateSchemaNode : public SchemaNode {
protected:
    CreateSchemaNode(ExecutionPlan* plan,
                     Kind kind,
                     GraphSpaceID space,
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SchemaNode(plan, kind, space)
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

class CreateTagNode final : public CreateSchemaNode {
public:
    static CreateTagNode* make(ExecutionPlan* plan,
                           GraphSpaceID space,
                           std::string tagName,
                           meta::cpp2::Schema schema,
                           bool ifNotExists) {
    return new CreateTagNode(plan,
                         space,
                         std::move(tagName),
                         std::move(schema),
                         ifNotExists);
    }

    std::string explain() const override {
        return "CreateTagNode";
    }

private:
    CreateTagNode(ExecutionPlan* plan,
              GraphSpaceID space,
              std::string tagName,
              meta::cpp2::Schema schema,
              bool ifNotExists)
        : CreateSchemaNode(plan,
                           Kind::kCreateTag,
                           space,
                           std::move(tagName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class CreateEdgeNode final : public CreateSchemaNode {
public:
    static CreateEdgeNode* make(ExecutionPlan* plan,
                            GraphSpaceID space,
                            std::string edgeName,
                            meta::cpp2::Schema schema,
                            bool ifNotExists) {
    return new CreateEdgeNode(plan,
                          space,
                          std::move(edgeName),
                          std::move(schema),
                          ifNotExists);
    }

    std::string explain() const override {
        return "CreateEdgeNode";
    }

private:
    CreateEdgeNode(ExecutionPlan* plan,
               GraphSpaceID space,
               std::string edgeName,
               meta::cpp2::Schema schema,
               bool ifNotExists)
        : CreateSchemaNode(plan,
                           Kind::kCreateEdge,
                           space,
                           std::move(edgeName),
                           std::move(schema),
                           ifNotExists) {
        }
};

class AlterTagNode final : public PlanNode {
public:
    std::string explain() const override {
        return "AlterTagNode";
    }
};

class AlterEdgeNode final : public PlanNode {
public:
    std::string explain() const override {
        return "AlterEdgeNode";
    }
};

class DescSchema : public PlanNode {
protected:
    DescSchema(ExecutionPlan* plan,
               Kind kind,
               GraphSpaceID space,
               std::string name)
        : PlanNode(plan, kind)
        , space_(space)
        , name_(std::move(name)) {}

public:
    const std::string& getName() const {
        return name_;
    }

    GraphSpaceID getSpaceId() const {
        return space_;
    }

protected:
    GraphSpaceID           space_;
    std::string            name_;
};

class DescTagNode final : public DescSchema {
public:
    static DescTagNode* make(ExecutionPlan* plan,
                         GraphSpaceID space,
                         std::string tagName) {
    return new DescTagNode(plan, space, std::move(tagName));
    }

    std::string explain() const override {
        return "DescTagNode";
    }

private:
    DescTagNode(ExecutionPlan* plan,
            GraphSpaceID space,
            std::string tagName)
        : DescSchema(plan, Kind::kDescTag, space, std::move(tagName)) {
        }
};

class DescEdgeNode final : public DescSchema {
public:
    static DescEdgeNode* make(ExecutionPlan* plan,
                          GraphSpaceID space,
                          std::string edgeName) {
    return new DescEdgeNode(plan, space, std::move(edgeName));
    }

    std::string explain() const override {
        return "DescEdgeNode";
    }

private:
    DescEdgeNode(ExecutionPlan* plan,
            GraphSpaceID space,
            std::string edgeName)
        : DescSchema(plan, Kind::kDescEdge, space, std::move(edgeName)) {
        }
};

class DropTagNode final : public PlanNode {
public:
    std::string explain() const override {
        return "DropTagNode";
    }
};

class DropEdgeNode final : public PlanNode {
public:
    std::string explain() const override {
        return "DropEdgeNode";
    }
};

class CreateTagIndex final : public PlanNode {
public:
    std::string explain() const override {
        return "CreateTagIndex";
    }
};

class CreateEdgeIndex final : public PlanNode {
public:
    std::string explain() const override {
        return "CreateEdgeIndex";
    }
};

class DescribeTagIndex final : public PlanNode {
public:
    std::string explain() const override {
        return "DescribeTagIndex";
    }
};

class DescribeEdgeIndex final : public PlanNode {
public:
    std::string explain() const override {
        return "DescribeEdgeIndex";
    }
};

class DropTagIndex final : public PlanNode {
public:
    std::string explain() const override {
        return "DropTagIndex";
    }
};

class DropEdgeIndex final : public PlanNode {
public:
    std::string explain() const override {
        return "DropEdgeIndex";
    }
};

class BuildTagIndex final : public PlanNode {
public:
    std::string explain() const override {
        return "BuildTagIndex";
    }
};

class BuildEdgeIndex final : public PlanNode {
public:
    std::string explain() const override {
        return "BuildEdgeIndex";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MAINTAIN_H_
