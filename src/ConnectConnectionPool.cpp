/*
 * predixy - A high performance and full features proxy for redis.
 * Copyright (C) 2017 Joyield, Inc. <joyield.com@gmail.com>
 * All rights reserved.
 */

#include "Proxy.h"
#include "ConnectConnectionPool.h"

ConnectConnectionPool::ConnectConnectionPool(Handler* h, Server* s, int dbnum):
    mHandler(h),
    mServ(s),
    mPendRequests(0),
    mShareConns(dbnum),
    mPrivateConns(dbnum),
    mLatencyMonitors(h->latencyMonitors())
{
    resetStats();
}

ConnectConnectionPool::~ConnectConnectionPool()
{
}

ConnectConnection* ConnectConnectionPool::getShareConnection(int db)
{
    FuncCallTimer();
    if (db >= (int)mShareConns.size()) {
        logWarn("h %d get share connection for db %d >= %d, server %s",
                mHandler->id(), db, (int)mShareConns.size(), mServ->addr().data());
        return nullptr;
    }
    bool needInit = false;
    ConnectConnection* c = mShareConns[db];
    if (!c) {
        c = ConnectConnectionAlloc::create(mServ, true);
        c->setDb(db);
        ++mStats.connections;
        mShareConns[db] = c;
        needInit = true;
        logNotice("h %d create server connection %s %d",
                  mHandler->id(), c->peer(), c->fd());
    } else if (c->fd() < 0) {
        logWarn("h %d getShareConnection: existing conn fd < 0, calling reopen: s %s, old_fd=%d",
                mHandler->id(), c->peer(), c->fd());
        if (mServ->fail()) {
            logWarn("h %d share conn for db %d: server %s marked as failed, cannot reopen connection",
                    mHandler->id(), db, mServ->addr().data());
            return nullptr;
        }
        c->reopen();
        needInit = true;
        logNotice("h %d reopen server connection %s %d",
                  mHandler->id(), c->peer(), c->fd());
    }
    if (needInit && !init(c)) {
        logWarn("h %d share conn for db %d: init failed for server %s %d",
                mHandler->id(), db, c->peer(), c->fd());
        c->close(mHandler);
        return nullptr;
    }
    if (mServ->fail()) {
        // Aggregate failed request logging to avoid log explosion
        static thread_local long lastFailLogTime = 0;
        static thread_local long failedReqCount = 0;

        long now = Util::nowUSec();
        failedReqCount++;

        // Log at most once per second with aggregated count
        if (now - lastFailLogTime > 1000000) {  // 1 second
            logWarn("h %d share conn for db %d: server %s marked as FAILED, rejected %ld requests in last second",
                    mHandler->id(), db, mServ->addr().data(), failedReqCount);
            lastFailLogTime = now;
            failedReqCount = 0;
        }

        return nullptr;
    }
    return c;
}

ConnectConnection* ConnectConnectionPool::getPrivateConnection(int db)
{
    FuncCallTimer();
    if (db >= (int)mPrivateConns.size()) {
        logWarn("h %d get private connection for db %d >= %d, server %s",
                mHandler->id(), db, (int)mPrivateConns.size(), mServ->addr().data());
        return nullptr;
    }
    auto& ccl = mPrivateConns[db];
    ConnectConnection* c = ccl.pop_front();
    bool needInit = false;
    if (!c) {
        if (mServ->fail()) {
            logWarn("h %d private conn for db %d: server %s marked as failed, cannot create new connection",
                    mHandler->id(), db, mServ->addr().data());
            return nullptr;
        }
        c = ConnectConnectionAlloc::create(mServ, false);
        c->setDb(db);
        ++mStats.connections;
        needInit = true;
        logNotice("h %d create private server connection %s %d",
                  mHandler->id(), c->peer(), c->fd());
    }
    if (c->fd() < 0) {
        if (mServ->fail()) {
            logWarn("h %d private conn for db %d: server %s marked as failed, cannot reopen connection",
                    mHandler->id(), db, mServ->addr().data());
            return nullptr;
        }
        c->reopen();
        needInit = true;
        logNotice("h %d reopen server connection %s %d",
                  mHandler->id(), c->peer(), c->fd());
    }
    if (needInit && !init(c)) {
        logWarn("h %d private conn for db %d: init failed for server %s %d",
                mHandler->id(), db, c->peer(), c->fd());
        c->close(mHandler);
        ccl.push_back(c);
        return nullptr;
    }
    if (mServ->fail()) {
        // Aggregate failed request logging to avoid log explosion
        static thread_local long lastPrivateFailLogTime = 0;
        static thread_local long privateFailedReqCount = 0;

        long now = Util::nowUSec();
        privateFailedReqCount++;

        // Log at most once per second with aggregated count
        if (now - lastPrivateFailLogTime > 1000000) {  // 1 second
            logWarn("h %d private conn for db %d: server %s marked as FAILED, rejected %ld private requests in last second",
                    mHandler->id(), db, mServ->addr().data(), privateFailedReqCount);
            lastPrivateFailLogTime = now;
            privateFailedReqCount = 0;
        }

        return nullptr;
    }
    return c;
}

void ConnectConnectionPool::putPrivateConnection(ConnectConnection* s)
{
    logDebug("h %d put private connection s %s %d",
            mHandler->id(), s->peer(), s->fd());
    unsigned db = s->db();
    if (db < mPrivateConns.size()) {
        mPrivateConns[db].push_back(s);
    } else {
        logWarn("h %d s %s %d put to pool with db %d invalid",
                mHandler->id(), s->peer(), s->fd(), s->db());
    }
}

bool ConnectConnectionPool::init(ConnectConnection* c)
{
    if (!c->setNonBlock()) {
        logWarn("h %d s %s %d set non block fail",
                mHandler->id(), c->peer(), c->fd());
        return false;
    }
    if (!c->setTcpNoDelay()) {
        logWarn("h %d s %s %d settcpnodelay fail %s",
                mHandler->id(), c->peer(), c->fd(), StrError());
    }
    auto sp = mHandler->proxy()->serverPool();
    if (sp->keepalive() > 0 && !c->setTcpKeepAlive(sp->keepalive())) {
        logWarn("h %d s %s %d settcpkeepalive(%d) fail %s",
                mHandler->id(), c->peer(), c->fd(), sp->keepalive(),StrError());
    }
    auto m = mHandler->eventLoop();
    if (!m->addSocket(c, Multiplexor::ReadEvent|Multiplexor::WriteEvent)) {
        logWarn("h %d s %s %d add to eventloop fail",
                mHandler->id(), c->peer(), c->fd());
        return false;
    }
    ++mStats.connect;

    int fdBeforeConnect = c->fd();
    int statusBeforeConnect = c->status();
    logWarn("h %d s %s %d init() calling connect(): fd=%d, status=%d (%s)",
            mHandler->id(), c->peer(), fdBeforeConnect, fdBeforeConnect,
            statusBeforeConnect, c->statusStr());

    bool connectResult = c->connect();
    int fdAfterConnect = c->fd();
    int statusAfterConnect = c->status();
    bool isConnecting = c->isConnecting();

    logWarn("h %d s %s %d init() connect() returned: result=%s, fd=%d, status=%d (%s), isConnecting=%d",
            mHandler->id(), c->peer(), fdAfterConnect, connectResult ? "true" : "false",
            fdAfterConnect, statusAfterConnect, c->statusStr(), isConnecting);

    if (!connectResult) {
        logError("h %d s %s %d TCP connect FAILED: %s",
                 mHandler->id(), c->peer(), c->fd(), StrError());
        m->delSocket(c);
        return false;
    }

    // Distinguish between Connected and Connecting
    if (isConnecting) {
        logWarn("h %d s %s %d TCP connect initiated (EINPROGRESS), waiting for WriteEvent to complete connection, sending AUTH/READONLY/PING...",
                mHandler->id(), c->peer(), c->fd());
    } else {
        logWarn("h %d s %s %d TCP connect completed immediately (rare), sending AUTH/READONLY/PING...",
                mHandler->id(), c->peer(), c->fd());
    }
    if (mServ->password().empty()) {
        c->setAuth(true);
    } else {
        c->setAuth(false);
        RequestPtr req = RequestAlloc::create();
        req->setAuth(mServ->password());
        mHandler->handleRequest(req, c);
        logWarn("h %d s %s %d sent AUTH command (req id=%ld)",
                mHandler->id(), c->peer(), c->fd(), req->id());
    }
    if (sp->type() == ServerPool::Cluster) {
        RequestPtr req = RequestAlloc::create(Request::Readonly);
        mHandler->handleRequest(req, c);
        logWarn("h %d s %s %d sent READONLY command (req id=%ld)",
                mHandler->id(), c->peer(), c->fd(), req->id());
    }
    int db = c->db();
    if (db != 0) {
        RequestPtr req = RequestAlloc::create();
        req->setSelect(db);
        mHandler->handleRequest(req, c);
        logWarn("h %d s %s %d sent SELECT %d command (req id=%ld)",
                mHandler->id(), c->peer(), c->fd(), db, req->id());
    }
    RequestPtr req = RequestAlloc::create(Request::PingServ);
    mHandler->handleRequest(req, c);
    logWarn("h %d s %s %d sent PING command (req id=%ld), waiting for PONG to restore server",
             mHandler->id(), c->peer(), c->fd(), req->id());
    return true;
}

void ConnectConnectionPool::check()
{
    FuncCallTimer();

    // Check server status
    if (!mServ->fail()) {
        return;  // Server is healthy, no need to check
    }

    if (!mServ->online()) {
        // Log offline status periodically to avoid log explosion
        static thread_local long lastOfflineLogTime = 0;
        long now = Util::nowUSec();
        if (now - lastOfflineLogTime > 60000000) {  // Every 60 seconds
            logWarn("h %d server %s is OFFLINE, skip reconnection check",
                    mHandler->id(), mServ->addr().data());
            lastOfflineLogTime = now;
        }
        return;
    }

    // Server is FAILED but ONLINE, attempt to activate for reconnection
    logWarn("h %d server %s is FAILED, attempting activate() to check reconnection window",
            mHandler->id(), mServ->addr().data());

    if (mServ->activate()) {
        // activate() succeeded, proceed with reconnection
        logWarn("h %d server %s activate() returned TRUE, proceeding with reconnection",
                mHandler->id(), mServ->addr().data());

        auto c = mShareConns.empty() ? nullptr : mShareConns[0];
        if (!c) {
            logError("h %d server %s mShareConns is empty, cannot reconnect",
                     mHandler->id(), mServ->addr().data());
            return;
        }

        int oldFd = c->fd();

        // Check connection state - this is CRITICAL for debugging the 66-second bug
        if (oldFd >= 0) {
            logWarn("h %d server %s connection fd=%d still valid, checking if good (potential bug: fd not properly closed)",
                    mHandler->id(), mServ->addr().data(), oldFd);

            // Check if connection is actually usable
            if (c->good()) {
                logWarn("h %d server %s connection fd=%d is good, skip reopen",
                        mHandler->id(), mServ->addr().data(), oldFd);
                return;
            } else {
                logWarn("h %d server %s connection fd=%d NOT good (status=%d %s), will force close and reopen",
                        mHandler->id(), mServ->addr().data(), oldFd,
                        c->status(), c->statusStr());
                // Note: we don't force close here to maintain original logic
                // but the log will help us identify if this is the issue
            }
        } else {
            logWarn("h %d server %s connection fd=%d invalid, will reopen",
                    mHandler->id(), mServ->addr().data(), oldFd);
        }

        // Reopen connection
        c->reopen();
        int newFd = c->fd();
        logWarn("h %d server %s reopen connection: old_fd=%d, new_fd=%d, peer=%s",
                mHandler->id(), mServ->addr().data(), oldFd, newFd, c->peer());

        // Initialize connection (send AUTH/READONLY/PING)
        logWarn("h %d server %s calling init() to send AUTH/READONLY/PING",
                mHandler->id(), mServ->addr().data());

        if (!init(c)) {
            logError("h %d server %s init() FAILED, closing connection fd=%d",
                     mHandler->id(), mServ->addr().data(), c->fd());
            c->close(mHandler);
        } else {
            logWarn("h %d server %s init() SUCCESS, waiting for PONG to restore",
                    mHandler->id(), mServ->addr().data());
        }
    } else {
        // activate() failed, reconnection window not ready yet
        logWarn("h %d server %s activate() returned FALSE, reconnection window not ready yet",
                mHandler->id(), mServ->addr().data());
    }
}
