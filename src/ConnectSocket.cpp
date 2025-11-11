/*
 * predixy - A high performance and full features proxy for redis.
 * Copyright (C) 2017 Joyield, Inc. <joyield.com@gmail.com>
 * All rights reserved.
 */

#include "ConnectSocket.h"
#include "Logger.h"


ConnectSocket::ConnectSocket(const char* peer, int type, int protocol):
    mPeer(peer),
    mType(type),
    mProtocol(protocol)
{
    mClassType = ConnectType;
    mPeerAddrLen = sizeof(mPeerAddr);
    getFirstAddr(peer, type, protocol, (sockaddr*)&mPeerAddr, &mPeerAddrLen);
    sockaddr* in = (sockaddr*)&mPeerAddr;
    int fd = Socket::socket(in->sa_family, type, protocol);
    attach(fd);
    mStatus = Unconnected;
}

bool ConnectSocket::connect()
{
    int currentFd = fd();
    int currentStatus = mStatus;
    logWarn("ConnectSocket::connect() called: peer=%s, fd=%d, status_before=%d (%s)",
            mPeer.c_str(), currentFd, currentStatus, statusStr());

    if (mStatus == Connecting || mStatus == Connected) {
        logWarn("ConnectSocket::connect() already connecting/connected: status=%d (%s), returning true",
                mStatus, statusStr());
        return true;
    }

    bool retry;
    do {
        retry = false;
        int ret = ::connect(fd(), (const sockaddr*)&mPeerAddr, mPeerAddrLen);
        int savedErrno = errno;

        logWarn("ConnectSocket::connect() ::connect() returned: peer=%s, fd=%d, ret=%d, errno=%d (%s)",
                mPeer.c_str(), fd(), ret, savedErrno, ret < 0 ? strerror(savedErrno) : "success");

        if (ret == 0) {
            mStatus = Connected;
            logWarn("ConnectSocket::connect() immediate success: fd=%d, status=Connected",
                    fd());
        } else {
            if (savedErrno == EINPROGRESS || savedErrno == EALREADY) {
                mStatus = Connecting;
                logWarn("ConnectSocket::connect() EINPROGRESS: fd=%d, status=Connecting (non-blocking connect in progress)",
                        fd());
            } else if (savedErrno == EISCONN) {
                mStatus = Connected;
                logWarn("ConnectSocket::connect() EISCONN: fd=%d, status=Connected (already connected)",
                        fd());
            } else if (savedErrno == EINTR) {
                retry = true;
                logWarn("ConnectSocket::connect() EINTR: fd=%d, retrying", fd());
            } else {
                mStatus = Unconnected;
                logError("ConnectSocket::connect() FAILED: peer=%s, fd=%d, errno=%d (%s), status=Unconnected",
                         mPeer.c_str(), fd(), savedErrno, strerror(savedErrno));
                return false;
            }
        }
    } while (retry);

    logWarn("ConnectSocket::connect() completed: peer=%s, fd=%d, status_after=%d (%s), returning true",
            mPeer.c_str(), fd(), mStatus, statusStr());
    return true;
}

void ConnectSocket::reopen()
{
    int currentFd = fd();
    int currentStatus = mStatus;
    logWarn("ConnectSocket::reopen() called: peer=%s, current_fd=%d, status=%d (%s)",
            mPeer.c_str(), currentFd, currentStatus, statusStr());

    if (fd() >= 0) {
        logError("ConnectSocket::reopen() SKIPPED: fd=%d >= 0 (BUG: fd not properly closed!)",
                 currentFd);
        return;
    }

    logWarn("ConnectSocket::reopen() creating new socket for peer=%s", mPeer.c_str());
    sockaddr* in = (sockaddr*)&mPeerAddr;
    int newFd = Socket::socket(in->sa_family, mType, mProtocol);
    attach(newFd);
    mStatus = Unconnected;
    logWarn("ConnectSocket::reopen() completed: peer=%s, old_fd=%d, new_fd=%d, status=%d (%s)",
            mPeer.c_str(), currentFd, newFd, mStatus, statusStr());
}

void ConnectSocket::close()
{
    mStatus = Disconnected;
    Socket::close();
}
