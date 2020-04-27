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
class CreateSpace final : public PlanNode {
public:
    static CreateSpace* make(ExecutionPlan* plan,
                             meta::SpaceDesc props,
                             bool ifNotExists) {
    return new CreateSpace(plan,
                           std::move(props),
                           ifNotExists);
    }

    std::string explain() const override {
        return "CreateSpace";
    }

public:
    meta::SpaceDesc getSpaceDesc() const {
        return props_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

private:
    CreateSpace(ExecutionPlan* plan,
                meta::SpaceDesc props,
                bool ifNotExists)
        : PlanNode(plan) {
            kind_ = Kind::kCreateSpace;
            props_ = std::move(props);
            ifNotExists_ = ifNotExists;
        }


private:
    meta::SpaceDesc               props_;
    bool                          ifNotExists_;
};

class SchemaNode : public PlanNode {
public:
    GraphSpaceID space() const {
        return space_;
    }

protected:
    SchemaNode(ExecutionPlan* plan, const GraphSpaceID space)
        : PlanNode(plan), space_(space) {}

protected:
    GraphSpaceID        space_;
};

class CreateSchemaNode : public SchemaNode {
protected:
    CreateSchemaNode(ExecutionPlan* plan,
                     GraphSpaceID space,
                     std::string name,
                     meta::cpp2::Schema schema,
                     bool ifNotExists)
        : SchemaNode(plan, space)
        , name_(std::move(name))
        , schema_(std::move(schema))
        , ifNotExists_(ifNotExists) {}

public:
    std::string getName() const {
        return name_;
    }

    meta::cpp2::Schema getSchema() const {
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
                           GraphSpaceID space,
                           std::string tagName,
                           meta::cpp2::Schema schema,
                           bool ifNotExists) {
    return new CreateTag(plan,
                         space,
                         std::move(tagName),
                         std::move(schema),
                         ifNotExists);
    }

    std::string explain() const override {
        return "CreateTag";
    }

private:
    CreateTag(ExecutionPlan* plan,
              GraphSpaceID space,
              std::string tagName,
              meta::cpp2::Schema schema,
              bool ifNotExists)
        : CreateSchemaNode(plan, space, std::move(tagName), std::move(schema), ifNotExists) {
            kind_ = Kind::kCreateTag;
        }
};

class CreateEdge final : public CreateSchemaNode {
public:
    static CreateEdge* make(ExecutionPlan* plan,
                            GraphSpaceID space,
                            std::string edgeName,
                            meta::cpp2::Schema schema,
                            bool ifNotExists) {
    return new CreateEdge(plan,
                          space,
                          std::move(edgeName),
                          std::move(schema),
                          ifNotExists);
    }

    std::string explain() const override {
        return "CreateEdge";
    }

private:
    CreateEdge(ExecutionPlan* plan,
               GraphSpaceID space,
               std::string edgeName,
               meta::cpp2::Schema schema,
               bool ifNotExists)
        : CreateSchemaNode(plan, space, std::move(edgeName), std::move(schema), ifNotExists) {
            kind_ = Kind::kCreateEdge;
        }
};

class AlterTag final : public PlanNode {
};

class AlterEdge final : public PlanNode {
};

class DescSchema : public PlanNode {
protected:
    DescSchema(ExecutionPlan* plan,
               GraphSpaceID space,
               std::string name)
        : PlanNode(plan)
        , space_(space)
        , name_(std::move(name)) {}

public:
    std::string getName() const {
        return name_;
    }

    GraphSpaceID getSpaceId() const {
        return space_;
    }

protected:
    GraphSpaceID           space_;
    std::string            name_;
};

class DescTag final : public DescSchema {
public:
    static DescTag* make(ExecutionPlan* plan,
                         GraphSpaceID space,
                         std::string tagName) {
    return new DescTag(plan, space, std::move(tagName));
    }

    std::string explain() const override {
        return "DescTag";
    }

private:
    DescTag(ExecutionPlan* plan,
            GraphSpaceID space,
            std::string tagName)
        : DescSchema(plan, space, std::move(tagName)) {
            kind_ = Kind::kDescTag;
        }
};

class DescEdge final : public DescSchema {
public:
    static DescEdge* make(ExecutionPlan* plan,
                          GraphSpaceID space,
                          std::string edgeName) {
    return new DescEdge(plan, space, std::move(edgeName));
    }

    std::string explain() const override {
        return "DescEdge";
    }

private:
    DescEdge(ExecutionPlan* plan,
            GraphSpaceID space,
            std::string edgeName)
        : DescSchema(plan, space, std::move(edgeName)) {
            kind_ = Kind::kDescEdge;
        }
};

class DescSpace final : public PlanNode {
public:
    static DescSpace* make(ExecutionPlan* plan,
                           std::string spaceName) {
    return new DescSpace(plan, std::move(spaceName));
    }

    std::string explain() const override {
        return "DescSpace";
    }

    std::string getSpaceName() const {
        return spaceName_;
    }

private:
    DescSpace(ExecutionPlan* plan,
              std::string spaceName)
        : PlanNode(plan) {
            kind_ = Kind::kDescEdge;
            spaceName_ = std::move(spaceName);
        }

private:
    std::string           spaceName_;
};

class DropTag final : public PlanNode {
};

class DropEdge final : public PlanNode {
};

class DropSpace final : public PlanNode {
};

class CreateTagIndex final : public PlanNode {
};

class CreateEdgeIndex final : public PlanNode {
};

class DescribeTagIndex final : public PlanNode {
};

class DescribeEdgeIndex final : public PlanNode {
};

class DropTagIndex final : public PlanNode {
};

class DropEdgeIndex final : public PlanNode {
};

class BuildTagIndex final : public PlanNode {
};

class BuildEdgeIndex final : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MAINTAIN_H_
