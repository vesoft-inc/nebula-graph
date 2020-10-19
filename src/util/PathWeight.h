/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_PATH_WEIGHT_H_
#define UTIL_PATH_WEIGHT_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"

namespace nebula {
namespace graph {


class QueryContext;

class IWeight {
public:
    explicit IWeight(QueryContext* qctx) : qctx_(qctx) {}
    virtual int64_t getWeight(const Value& src, const Value& dst, const Value& edge) = 0;
    virtual ~IWeight() = default;

protected:
    QueryContext* qctx_{nullptr};
};

class NoWeight final : public IWeight {
public:
    explicit NoWeight(QueryContext* qctx) : IWeight(qctx) {}
    int64_t getWeight(const Value &src, const Value& dst, const Value& edge)  override {
        UNUSED(src);
        UNUSED(dst);
        UNUSED(edge);
        return 1;
    }
};

class TagWeight final : public IWeight {
public:
    explicit TagWeight(QueryContext* qctx) : IWeight(qctx) {}
    int64_t getWeight(const Value& src, const Value& dst, const Value& edge) override {
        // todo
        LOG(FATAL) << "not implement " << src << " " << dst;
    }
    void setProp(const std::string& tagName, const std::string& propName) {
        tagName_ = std::move(tagName);
        propName_ = std::move(propName);
    }

private:
    std::string tagName_;
    std::string propName_;
};

class EdgeWeight final : public IWeight {
public:
    explicit EdgeWeight(QueryContext* qctx) : IWeight(qctx) {}
    int64_t getWeight(const Value& src, const Value& dst) override {
        // todo
        LOG(FATAL) << "not implement " << src << " " << dst;
    }
    void setProp(const std::string& edgeName, const std::string& propName) {
        edgeName_ = std::move(edgeName);
        propName_ = std::move(propName);
    }

private:
    std::string edgeName_;
    std::string propName_;
};

}  // namespace graph
}  // namespace nebula

#endif  // UTIL_PATH_WEIGHT_H_
