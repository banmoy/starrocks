// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>
#include <string_view>

#include "common/config.h"
#include "gutil/macros.h"

namespace starrocks {

class Cache;

class FdCache {
public:
    struct Handle {};

    static FdCache* Instance() {
        static FdCache cache(std::max<size_t>(4096, config::file_descriptor_cache_capacity));
        return &cache;
    }

    ~FdCache();

    DISALLOW_COPY_AND_ASSIGN(FdCache);

    // Insert a mapping from path->fd into the cache.
    //
    // Returns a handle that corresponds to the mapping.  The caller
    // must call this->release(handle) when the returned mapping is no
    // longer needed.
    //
    // When the inserted entry is no longer needed, the file descriptor
    // will be `close`d.
    Handle* insert(std::string_view path, int fd);

    // If the cache has no mapping for "path", returns NULL.
    //
    // Else return a handle that corresponds to the mapping.  The caller
    // must call this->release(handle) when the returned mapping is no
    // longer needed.
    Handle* lookup(std::string_view path);

    // If the cache contains entry for path, erase it.  Note that the
    // underlying entry will be kept around until all existing handles
    // to it have been released.
    void erase(std::string_view path);

    // Release a mapping returned by a previous lookup().
    // REQUIRES: handle must not have been released yet.
    void release(Handle* handle);

    // Remove all cache entries that are not actively in use.
    void prune();

    // Return the file descriptor encapsulated in a handle returned by a
    // successful lookup().
    // REQUIRES: handle must not have been released yet.
    static int fd(Handle* handle);

private:
    explicit FdCache(size_t capacity);

    Cache* _cache;
};

} // namespace starrocks
