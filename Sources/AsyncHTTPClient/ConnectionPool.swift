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

import Foundation
import NIO
import NIOConcurrencyHelpers
import NIOHTTP1
import NIOTLS

/// A connection pool that manages and creates new connections to hosts respecting the specified preferences
class ConnectionPool {
    /// The default look group used to schedule async work
    let loopGroup: EventLoopGroup

    /// The configuration used to bootstrap new HTTP connections
    let configuration: HTTPClient.Configuration

    /// The main data structure used by the `ConnectionPool` to retreive and create connections associated
    /// to a a given `Key` .
    /// - Warning: This property shouldn't be directly accessed, use the `ConnectionPool` subscript instead
    var _connectionProviders: [Key: ConnectionProvider] = [:]

    private let lock = Lock()

    init(group: EventLoopGroup, configuration: HTTPClient.Configuration) {
        self.loopGroup = group
        self.configuration = configuration
    }

    /// This enables the connection pool to store and retreive `ConnectionProvider`s
    /// by ensuring thread safety using the `lock`.
    private subscript(key: Key) -> ConnectionProvider? {
        get {
            return self.lock.withLock {
                _connectionProviders[key]
            }
        }
        set {
            self.lock.withLock {
                _connectionProviders[key] = newValue
            }
        }
    }

    private enum RequiredAction {
        case make(EventLoopPromise<ConnectionProvider>)
        case existing(ConnectionProvider)
    }

    /// Determines what action should be taken regarding the the `ConnectionProvider`
    ///
    /// This method ensures there is no race condition if called concurently from multiple threads
    private func getAction(for key: Key, eventLoop: EventLoop) -> RequiredAction {
        return self.lock.withLock {
            if let provider = self._connectionProviders[key] {
                return .existing(provider)
            } else {
                let promise = eventLoop.makePromise(of: ConnectionProvider.self)
                promise.futureResult.whenComplete { result in
                    switch result {
                    case .success(let provider):
                        self[key] = provider
                    case .failure:
                        self[key] = nil
                    }
                }
                return .make(promise)
            }
        }
    }

    /// This method asks the pool for a connection usable by the specified `request`, respecting the specified options.
    ///
    /// - parameter request: The request that needs a `Connection`
    /// - parameter preference: The `EventLoopPreference` the connection pool will respect to lease a new connection
    /// - parameter deadline: The connection timeout
    /// - Returns: A connection  corresponding to the specified parameters
    ///
    /// When the pool is asked for a new connection, it creates a `Key` from the url associated to the `request`. This key
    /// is used to determine if there already exists an associated `ConnectionProvider` in `connectionProviders`
    /// if it does, the connection provider then takes care of leasing a new connection. If a connection provider doesn't exist, it is created.
    func getConnection(for request: HTTPClient.Request, eventLoop: EventLoop, deadline: NIODeadline?) -> EventLoopFuture<Connection> {
        let key = Key(request: request)

        switch self.getAction(for: key, eventLoop: eventLoop) {
        case .existing(let provider):
            return provider.getConnection(on: eventLoop)

        case .make(let providerPromise):
            // If no connection provider exists for the given key, create a new one
            let selectedEventLoop = providerPromise.futureResult.eventLoop
            self[key] = .future(providerPromise.futureResult)
            var bootstrap = ClientBootstrap.makeHTTPClientBootstrapBase(group: selectedEventLoop, host: request.host, port: request.port, configuration: self.configuration) { _ in
                self.loopGroup.next().makeSucceededFuture(())
            }

            if let timeout = resolve(timeout: self.configuration.timeout.connect, deadline: deadline) {
                bootstrap = bootstrap.connectTimeout(timeout)
            }

            let address = HTTPClient.resolveAddress(host: request.host, port: request.port, proxy: self.configuration.proxy)

            return bootstrap.connect(host: address.host, port: address.port).flatMap { channel in
                let http1PipelineConfigurator: (ChannelPipeline) -> EventLoopFuture<ConnectionProvider> = { pipeline in
                    pipeline.addHTTPClientHandlers(leftOverBytesStrategy: .forwardBytes).map { _ in
                        let http1Provider = ConnectionProvider.http1(HTTP1ConnectionProvider(group: self.loopGroup, key: key, configuration: self.configuration, initialConnection: Connection(key: key, channel: channel, parentPool: self), parentPool: self))
                        return http1Provider
                    }
                }

                return channel.pipeline.addSSLHandlerIfNeeded(for: key, tlsConfiguration: self.configuration.tlsConfiguration).flatMap { _ in
                    http1PipelineConfigurator(channel.pipeline)
                }.flatMap { provider in
                    providerPromise.succeed(provider)
                    return provider.getConnection(on: eventLoop)
                }
            }
        }
    }

    func release(_ connection: Connection) {
        self.lock.withLock {
            if let provider = self._connectionProviders[connection.key] {
                provider.release(connection: connection)
            }
        }
    }

    func closeAllConnections() -> EventLoopFuture<Void> {
        return self.lock.withLock {
            let closeFutures = _connectionProviders.values.map { $0.closeAllConnections() }
            return EventLoopFuture<Void>.andAllSucceed(closeFutures, on: loopGroup.next())
        }
    }

    /// Used by the `ConnectionPool` to index its `ConnectionProvider`s
    ///
    /// A key is initialized from a `URL`, extracting, validating and storing its scheme,
    /// host and port components. It uses the components to derive a hashed value
    /// used by the `connectionProviders` dictionary to allow retreiving and
    /// creating connection providers associated to a certain request in constant time.
    struct Key: Hashable {
        init(request: HTTPClient.Request) {
            switch request.scheme {
            case "http":
                self.scheme = .http
            case "https":
                self.scheme = .https
            default:
                fatalError("HTTPClient.Request scheme should already be a valid one")
            }
            self.port = request.port
            self.host = request.host
        }

        var scheme: Scheme
        var host: String
        var port: Int

        enum Scheme: Hashable {
            case http
            case https
        }
    }

    class Connection {
        init(key: Key, channel: Channel, parentPool: ConnectionPool) {
            self.key = key
            self.channel = channel
            self.parentPool = parentPool
        }

        func release() {
            self.parentPool.release(self)
        }

        let parentPool: ConnectionPool
        fileprivate let key: Key
        let channel: Channel
        var isLeased: Bool = false
    }

    /// This enum abstracts the underlying kind of provider
    enum ConnectionProvider {
        case http1(HTTP1ConnectionProvider)
        indirect case future(EventLoopFuture<ConnectionProvider>)

        func getConnection(on eventLoop: EventLoop) -> EventLoopFuture<Connection> {
            switch self {
            case .http1(let provider):
                return provider.getConnection(preference: .delegate(on: eventLoop))
            case .future(let futureProvider):
                return futureProvider.flatMap { provider in
                    provider.getConnection(on: eventLoop)
                }
            }
        }

        func release(connection: Connection) {
            switch self {
            case .http1(let provider):
                return provider.release(connection: connection)
            case .future(let futureProvider):
                futureProvider.whenSuccess { provider in
                    provider.release(connection: connection)
                }
            }
        }

        func closeAllConnections() -> EventLoopFuture<Void> {
            switch self {
            case .http1(let provider):
                return provider.closeAllConnections()
            case .future(let futureProvider):
                return futureProvider.flatMap { provider in
                    provider.closeAllConnections()
                }
            }
        }
    }

    class HTTP1ConnectionProvider {
        let loopGroup: EventLoopGroup
        let configuration: HTTPClient.Configuration
        let key: ConnectionPool.Key
        var isClosed: Atomic<Bool>
        /// The curently opened, unleased connections in this connection provider
        var availableConnections: CircularBuffer<Connection> = .init(initialCapacity: 8) {
            didSet {
                self.closeIfNeeded()
            }
        }

        /// Consumers that weren't able to get a new connection without exceeding
        /// `maximumConcurrentConnections` get a `Future<Connection>`
        /// whose associated promise is stored in `Waiter`. The promise is completed
        /// as soon as possible by the provider, in FIFO order.
        var waiters: CircularBuffer<Waiter> = .init()

        var leased: Int = 0 {
            didSet {
                self.closeIfNeeded()
            }
        }

        let maximumConcurrentConnections: Int = 8

        let lock = Lock()

        let parentPool: ConnectionPool

        init(group: EventLoopGroup, key: ConnectionPool.Key, configuration: HTTPClient.Configuration, initialConnection: Connection, parentPool: ConnectionPool) {
            self.loopGroup = group
            self.configuration = configuration
            self.key = key
            self.availableConnections.append(initialConnection)
            self.parentPool = parentPool
            self.isClosed = Atomic(value: false)
            self.configureClose(of: initialConnection)
        }

        deinit {
            assert(availableConnections.isEmpty, "Available connections should be empty before deinit")
            assert(leased == 0, "All leased connections should have been returned before deinit")
        }

        /// Get a new connection from this provider
        func getConnection(preference: HTTPClient.EventLoopPreference) -> EventLoopFuture<Connection> {
            return self.lock.withLock {
                if leased < maximumConcurrentConnections {
                    leased += 1
                    if let connection = availableConnections.popFirst(), connection.channel.isActive {
                        connection.isLeased = true
                        return self.loopGroup.next().makeSucceededFuture(connection)
                    } else {
                        return self.makeConnection()
                    }
                } else {
                    let promise = self.loopGroup.next().makePromise(of: Connection.self)
                    self.waiters.append(Waiter(promise: promise, preference: preference))
                    return promise.futureResult
                }
            }
        }

        private func configureClose(of connection: Connection) {
            connection.channel.closeFuture.whenComplete { _ in
                if !connection.isLeased {
                    if let indexToRemove = self.availableConnections.firstIndex(where: { $0 === connection }) {
                        self.availableConnections.swapAt(self.availableConnections.startIndex, indexToRemove)
                        self.availableConnections.removeFirst()
                    }
                } else {
                    connection.release()
                }
            }
        }

        private func makeConnection() -> EventLoopFuture<Connection> {
            let bootstrap = ClientBootstrap.makeHTTPClientBootstrapBase(group: self.loopGroup, host: self.key.host, port: self.key.port, configuration: self.configuration) { channel in
                channel.pipeline.addSSLHandlerIfNeeded(for: self.key, tlsConfiguration: self.configuration.tlsConfiguration).flatMap {
                    channel.pipeline.addHTTPClientHandlers(leftOverBytesStrategy: .forwardBytes)
                }
            }
            let address = HTTPClient.resolveAddress(host: self.key.host, port: self.key.port, proxy: self.configuration.proxy)
            return bootstrap.connect(host: address.host, port: address.port).map { channel in
                let connection = Connection(key: self.key, channel: channel, parentPool: self.parentPool)
                self.configureClose(of: connection)
                connection.isLeased = true
                return connection
            }
        }

        func release(connection: Connection) {
            self.lock.withLock {
                if connection.channel.isActive {
                    if let firstWaiter = waiters.popFirst() {
                        firstWaiter.promise.succeed(connection)
                    } else {
                        self.availableConnections.append(connection)
                        leased -= 1
                        connection.isLeased = false
                    }
                } else {
                    if let firstWaiter = waiters.popFirst() {
                        self.makeConnection().cascade(to: firstWaiter.promise)
                    } else {
                        leased -= 1
                    }
                }
            }
        }

        func closeAllConnections() -> EventLoopFuture<Void> {
            return self.lock.withLock {
                EventLoopFuture<Void>.andAllComplete(availableConnections.map { $0.channel.close() }, on: loopGroup.next()).map {
                    self.availableConnections.removeAll()
                }
            }
        }

        private func closeIfNeeded() {
            if !self.isClosed.load(), self.leased == 0, self.availableConnections.isEmpty, self.waiters.isEmpty {
                self.isClosed.store(true)
                self.parentPool._connectionProviders[key] = nil
            }
        }

        struct Waiter {
            let promise: EventLoopPromise<Connection>
            let preference: HTTPClient.EventLoopPreference
        }
    }
}
