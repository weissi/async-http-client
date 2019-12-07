//===----------------------------------------------------------------------===//
//
// This source file is part of the AsyncHTTPClient open source project
//
// Copyright (c) 2018-2019 Apple Inc. and the AsyncHTTPClient project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of AsyncHTTPClient project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO
import NIOHTTP1
import NIOHTTPCompression

public final class HTTPClientCopyingDelegate: HTTPClientResponseDelegate {
    public typealias Response = Void

    let chunkHandler: (ByteBuffer) -> EventLoopFuture<Void>

    init(chunkHandler: @escaping (ByteBuffer) -> EventLoopFuture<Void>) {
        self.chunkHandler = chunkHandler
    }

    public func didReceiveBodyPart(task: HTTPClient.Task<Void>, _ buffer: ByteBuffer) -> EventLoopFuture<Void> {
        return self.chunkHandler(buffer)
    }

    public func didFinishRequest(task: HTTPClient.Task<Void>) throws {
        return ()
    }
}

extension ClientBootstrap {
    static func makeHTTPClientBootstrapBase(group: EventLoopGroup, host: String, port: Int, configuration: HTTPClient.Configuration, channelInitializer: ((Channel) -> EventLoopFuture<Void>)? = nil) -> ClientBootstrap {
        return ClientBootstrap(group: group)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(IPPROTO_TCP), TCP_NODELAY), value: 1)

            .channelInitializer { channel in
                let channelAddedFuture: EventLoopFuture<Void>
                switch configuration.proxy {
                case .none:
                    channelAddedFuture = group.next().makeSucceededFuture(())
                case .some:
                    channelAddedFuture = channel.pipeline.addProxyHandler(host: host, port: port, authorization: configuration.proxy?.authorization)
                }
                return channelAddedFuture.flatMap { (_: Void) -> EventLoopFuture<Void> in
                    channelInitializer?(channel) ?? group.next().makeSucceededFuture(())
                }
            }
    }
}

func resolve(timeout: TimeAmount?, deadline: NIODeadline?) -> TimeAmount? {
    switch (timeout, deadline) {
    case (.some(let timeout), .some(let deadline)):
        return min(timeout, deadline - .now())
    case (.some(let timeout), .none):
        return timeout
    case (.none, .some(let deadline)):
        return deadline - .now()
    case (.none, .none):
        return nil
    }
}

extension CircularBuffer {
    @discardableResult
    mutating func swapRemove(at index: Index) -> Element? {
        precondition(index >= self.startIndex && index < self.endIndex)
        if !self.isEmpty {
            self.swapAt(self.startIndex, index)
            return self.removeFirst()
        } else {
            return nil
        }
    }

    @discardableResult
    mutating func swapRemove(where predicate: (Element) throws -> Bool) rethrows -> Element? {
        if let existingIndex = try self.firstIndex(where: predicate) {
            return self.swapRemove(at: existingIndex)
        } else {
            return nil
        }
    }

    @discardableResult
    mutating func swapRemoveWhereOrFirst(where predicate: (Element) throws -> Bool) rethrows -> Element? {
        return try self.swapRemove(where: predicate) ?? self.popFirst()
    }
}
