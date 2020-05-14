/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_QUERYCONTEXT_H_
#define CONTEXT_QUERYCONTEXT_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include "common/meta/SchemaManager.h"
#include "common/cpp/helpers.h"
#include "common/charset/Charset.h"
#include "parser/SequentialSentences.h"
#include "service/RequestContext.h"
// #include "meta/ClientBasedGflagsManager.h"
#include "clients/meta/MetaClient.h"
#include "clients/storage/GraphStorageClient.h"
#include "util/ObjectPool.h"

namespace nebula {
namespace graph {
/***************************************************************************
 *
 * The context for each query request
 *
 * The context is NOT thread-safe. The execution plan has to guarantee
 * all accesses to context are safe
 *
 * The life span of the context is same as the request. That means a new
 * context object will be created as soon as the query engine receives the
 * query request. The context object will be visible to the parser, the
 * planner, the optimizer, and the executor.
 *
 **************************************************************************/
class QueryContext {
public:
    using RequestContextPtr = std::unique_ptr<RequestContext<cpp2::ExecutionResponse>>;

    QueryContext() = default;
    QueryContext(RequestContextPtr rctx,
                 meta::SchemaManager* sm,
                 // meta::ClientBasedGflagsManager *gflagsManager,
                 storage::GraphStorageClient* storage,
                 meta::MetaClient* metaClient,
                 CharsetInfo* charsetInfo)
        : rctx_(std::move(rctx)),
          sm_(sm),
          // gflagsManager_(gflagsManager),
          storageClient_(storage),
          metaClient_(metaClient),
          charsetInfo_(charsetInfo),
          objPool_(std::make_unique<ObjectPool>()) {
        DCHECK_NOTNULL(sm_);
        DCHECK_NOTNULL(storageClient_);
        DCHECK_NOTNULL(metaClient_);
        DCHECK_NOTNULL(charsetInfo_);
    }
    virtual ~QueryContext() = default;

    // Get the latest version of the value
    const Value& getValue(const std::string& name) const;

    size_t numVersions(const std::string& name) const;

    // Return all existing history of the value. The front is the latest value
    // and the back is the oldest value
    const std::vector<Value>& getHistory(const std::string& name) const;

    void setValue(const std::string& name, Value&& val);

    void deleteValue(const std::string& name);

    // Only keep the last several versoins of the Value
    void truncHistory(const std::string& name, size_t numVersionsToKeep);

    RequestContext<cpp2::ExecutionResponse>* rctx() const {
        return rctx_.get();
    }

    meta::SchemaManager* schemaManager() const {
        return sm_;
    }

    storage::GraphStorageClient* getStorageClient() const {
        return storageClient_;
    }

    meta::MetaClient* getMetaClient() const {
        return metaClient_;
    }

    CharsetInfo* getCharsetInfo() const {
        return charsetInfo_;
    }

    ObjectPool* objPool() const {
        return objPool_.get();
    }

private:
    RequestContextPtr                                       rctx_;
    meta::SchemaManager*                                    sm_{nullptr};
    // meta::ClientBasedGflagsManager             *gflagsManager_{nullptr};
    storage::GraphStorageClient*                            storageClient_{nullptr};
    meta::MetaClient*                                       metaClient_{nullptr};
    CharsetInfo*                                            charsetInfo_{nullptr};
    // The Object Poll holds all internal generated objects.
    std::unique_ptr<ObjectPool>                             objPool_;
    // name -> Value with multiple versions
    std::unordered_map<std::string, std::vector<Value>>     valueMap_;
};
}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_QUERYCONTEXT_H_
