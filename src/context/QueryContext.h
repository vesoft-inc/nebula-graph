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
#include "clients/meta/MetaClient.h"
#include "clients/storage/GraphStorageClient.h"
#include "util/ObjectPool.h"
#include "context/ValidateContext.h"
#include "context/ExecutionContext.h"

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

    QueryContext(RequestContextPtr rctx,
                 meta::SchemaManager* sm,
                 storage::GraphStorageClient* storage,
                 meta::MetaClient* metaClient,
                 CharsetInfo* charsetInfo)
        : rctx_(std::move(rctx)),
          sm_(sm),
          storageClient_(storage),
          metaClient_(metaClient),
          charsetInfo_(charsetInfo) {
        DCHECK_NOTNULL(sm_);
        DCHECK_NOTNULL(storageClient_);
        DCHECK_NOTNULL(metaClient_);
        DCHECK_NOTNULL(charsetInfo_);
        objPool_ = std::make_unique<ObjectPool>();
        ep_ = std::make_unique<ExecutionPlan>(objPool_.get());
        vctx_ = std::make_unique<ValidateContext>();
        ectx_ = std::make_unique<ExecutionContext>();
    }

    QueryContext() {
        objPool_ = std::make_unique<ObjectPool>();
        ep_ = std::make_unique<ExecutionPlan>(objPool_.get());
        vctx_ = std::make_unique<ValidateContext>();
        ectx_ = std::make_unique<ExecutionContext>();
    }

    virtual ~QueryContext() = default;

    void setRctx(RequestContextPtr rctx) {
        rctx_ = std::move(rctx);
    }

    void setSchemaManager(meta::SchemaManager* sm) {
        sm_ = sm;
    }

    void setStorageClient(storage::GraphStorageClient* storage) {
        storageClient_ = storage;
    }

    void setMetaClient(meta::MetaClient* metaClient) {
        metaClient_ = metaClient;
    }

    void setCharsetInfo(CharsetInfo* charsetInfo) {
        charsetInfo_ = charsetInfo;
    }

    RequestContext<cpp2::ExecutionResponse>* rctx() const {
        return rctx_.get();
    }

    ValidateContext* vctx() const {
        return vctx_.get();
    }

    ExecutionContext* ectx() const {
        return ectx_.get();
    }

    ExecutionPlan* plan() const {
        return ep_.get();
    }

    meta::SchemaManager* schemaMng() const {
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
    std::unique_ptr<ValidateContext>                        vctx_;
    std::unique_ptr<ExecutionContext>                       ectx_;
    std::unique_ptr<ExecutionPlan>                          ep_;
    meta::SchemaManager*                                    sm_{nullptr};
    // meta::ClientBasedGflagsManager             *gflagsManager_{nullptr};
    storage::GraphStorageClient*                            storageClient_{nullptr};
    meta::MetaClient*                                       metaClient_{nullptr};
    CharsetInfo*                                            charsetInfo_{nullptr};
    // The Object Pool holds all internal generated objects.
    // e.g. expressions, plan nodes, executors
    std::unique_ptr<ObjectPool>                             objPool_;
};
}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_QUERYCONTEXT_H_
