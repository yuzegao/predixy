/*
 * predixy - A high performance and full features proxy for redis.
 * Copyright (C) 2017 Joyield, Inc. <joyield.com@gmail.com>
 * All rights reserved.
 */

#include "Proxy.h"
#include "Server.h"
#include "ServerPool.h"

const char* Server::RoleStr[] = {
    "unknown",
    "master",
    "slave",
    "sentinel"
};

Server::Server(ServerPool* pool, const String& addr, bool isStatic):
    mPool(pool),
    mGroup(nullptr),
    mRole(Unknown),
    mDC(nullptr),
    mAddr(addr),
    mNextActivateTime(0),
    mFailureCnt(0),
    mStatic(isStatic),
    mFail(false),
    mOnline(true),
    mUpdating(false)
{
    if (auto dataCenter = pool->proxy()->dataCenter()) {
        mDC = dataCenter->getDC(addr);
    }
}

Server::~Server()
{
}

bool Server::activate()
{
    long v = mNextActivateTime;
    long now = Util::nowUSec();

    logWarn("server %s activate() called: now=%ld, mNextActivateTime=%ld, diff=%ld",
            mAddr.data(), now, v, now - v);

    if (now < v) {
        logWarn("server %s activate() returning FALSE: blocked by time window (need to wait %ld us)",
                mAddr.data(), v - now);
        return false;
    }

    bool result = AtomicCAS(mNextActivateTime, v, now + mPool->serverRetryTimeout());

    logWarn("server %s activate() CAS result=%s, new_mNextActivateTime=%ld",
            mAddr.data(), result ? "SUCCESS" : "FAILED", now + mPool->serverRetryTimeout());

    if (result) {
        long retryTimeout = mPool->serverRetryTimeout();
        logWarn("server %s activate() returning TRUE: CAS succeeded, next activation window after %ld us (failCnt=%ld)",
                mAddr.data(), retryTimeout, (long)mFailureCnt);
    } else {
        logWarn("server %s activate() returning FALSE: CAS failed (another handler won)",
                mAddr.data());
    }

    return result;
}

void Server::incrFail()
{
    long cnt = ++mFailureCnt;
    logWarn("server %s failure count incremented to %ld (limit=%d)",
            mAddr.data(), cnt, mPool->serverFailureLimit());
    if (cnt % mPool->serverFailureLimit() == 0) {
        setFail(true);
        logWarn("server %s marked as FAILED after %ld failures (limit=%d)",
                mAddr.data(), cnt, mPool->serverFailureLimit());
    }
}

void Server::setFail(bool v)
{
    bool oldFail = mFail;
    mFail = v;

    if (v && !oldFail) {
        // State change: ALIVE → FAILED
        logWarn("server %s state changed: ALIVE to FAILED (failCnt=%ld, ServerFailureLimit=%d)",
                mAddr.data(), (long)mFailureCnt, mPool->serverFailureLimit());
    } else if (!v && oldFail) {
        // State change: FAILED → ALIVE
        logWarn("server %s state changed: FAILED to ALIVE (failCnt=%ld, was failed, now restored)",
                mAddr.data(), (long)mFailureCnt);
    }
}

