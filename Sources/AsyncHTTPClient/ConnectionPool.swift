//===----------------------------------------------------------------------===//
//
// This source file is part of the AsyncHTTPClient open source project
//
// Copyright (c) 2019 Apple Inc. and the AsyncHTTPClient project authors
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
    /// The configuration used to bootstrap new HTTP connections
    private let configuration: HTTPClient.Configuration

    /// The main data structure used by the `ConnectionPool` to retreive and create connections associated
    /// to a a given `Key` .
    /// - Warning: This property shouldn't be directly accessed, use the `ConnectionPool` subscript instead
    var _connectionProviders: [Key: HTTP1ConnectionProvider] = [:]

    /// The lock used by the connection pool used to ensure correct synchronization of accesses to `_connectionProviders`
    ///
    /// Accesses that only need synchronization for the span of a get or set can use the subscript instead of this property
    private let connectionProvidersLock = Lock()

    init(configuration: HTTPClient.Configuration) {
        self.configuration = configuration
    }

    /// This enables the connection pool to store and retreive `HTTP1ConnectionProvider`s
    /// by ensuring thread safety using the `connectionProvidersLock`.
    private subscript(key: Key) -> HTTP1ConnectionProvider? {
        get {
            return self.connectionProvidersLock.withLock {
                _connectionProviders[key]
            }
        }
        set {
            self.connectionProvidersLock.withLock {
                _connectionProviders[key] = newValue
            }
        }
    }

    /// Gets the `EventLoop` associated to the given `Key` if it exists
    ///
    /// This is part of optimization used by the `.execute(...)` method when
    /// a request has its `EventLoopPreference` property set to `.indifferent`.
    /// Having a default `EventLoop` shared by the *channel* and the *delegate* avoids
    /// loss of performance due to `EventLoop` hoping
    func associatedEventLoop(for key: Key) -> EventLoop? {
        return self[key]?.eventLoop
    }

    /// This method asks the pool for a connection usable by the specified `request`, respecting the specified options.
    ///
    /// - parameter request: The request that needs a `Connection`
    /// - parameter preference: The `EventLoopPreference` the connection pool will respect to lease a new connection
    /// - parameter deadline: The connection timeout
    /// - Returns: A connection  corresponding to the specified parameters
    ///
    /// When the pool is asked for a new connection, it creates a `Key` from the url associated to the `request`. This key
    /// is used to determine if there already exists an associated `HTTP1ConnectionProvider` in `connectionProviders`
    /// if it does, the connection provider then takes care of leasing a new connection. If a connection provider doesn't exist, it is created.
    func getConnection(for request: HTTPClient.Request, preference: HTTPClient.EventLoopPreference, on eventLoop: EventLoop, deadline: NIODeadline?) -> EventLoopFuture<Connection> {
        let key = Key(request)

        let provider: HTTP1ConnectionProvider = self.connectionProvidersLock.withLock {
            if let existing = self._connectionProviders[key] {
                return existing
            } else {
                let http1Provider = HTTP1ConnectionProvider(key: key, eventLoop: eventLoop, configuration: self.configuration, parentPool: self)
                self._connectionProviders[key] = http1Provider
                return http1Provider
            }
        }

        return provider.getConnection(preference: preference)
    }

    func release(_ connection: Connection) {
        if let connectionProvider = self[connection.key] {
            connectionProvider.release(connection: connection)
        }
    }

    func syncClose(requiresCleanClose: Bool) throws {
        let connectionProviders = self.connectionProvidersLock.withLock { _connectionProviders.values }
        for connectionProvider in connectionProviders {
            try connectionProvider.syncClose(requiresCleanClose: requiresCleanClose)
        }
    }

    /// Used by the `ConnectionPool` to index its `HTTP1ConnectionProvider`s
    ///
    /// A key is initialized from a `URL`, it uses the components to derive a hashed value
    /// used by the `connectionProviders` dictionary to allow retreiving and creating
    /// connection providers associated to a certain request in constant time.
    struct Key: Hashable {
        init(_ request: HTTPClient.Request) {
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

    /// A `Connection` represents a `Channel` in the context of the connection pool
    ///
    /// In the `ConnectionPool`, each `Channel` belongs to a given `HTTP1ConnectionProvider`
    /// and has a certain "lease state" (see the `isLeased` property).
    /// The role of `Connection` is to model this by storing a `Channel` alongside its associated properties
    /// so that they can be passed around together.
    ///
    /// - Warning: `Connection` properties are not thread-safe and should be used with proper synchronization
    class Connection {
        init(key: Key, channel: Channel, parentPool: ConnectionPool) {
            self.key = key
            self.channel = channel
            self.parentPool = parentPool
        }

        /// Release this `Connection` to its associated `HTTP1ConnectionProvider` in the parent `ConnectionPool`
        ///
        /// This is exactly equivalent to calling `.release(theProvider)` on `ConnectionPool`
        ///
        /// - Warning: This only releases the connection and doesn't take care of cleaning handlers in th
        ///  `Channel` pipeline.
        func release() {
            self.parentPool.release(self)
        }

        /// The connection pool this `Connection` belongs to.
        ///
        /// This enables calling methods like `release()` directly on a `Connection` instead of
        /// calling `pool.release(connection)`. This gives a more object oriented feel to the API
        /// and can avoid having to keep explicit references to the pool at call site.
        let parentPool: ConnectionPool

        /// The `Key` of the `HTTP1ConnectionProvider` this `Connection` belongs to
        ///
        /// This lets `ConnectionPool` know the relationship between `Connection`s and `HTTP1ConnectionProvider`s
        fileprivate let key: Key

        /// The `Channel` of this `Connection`
        ///
        /// - Warning: Requests that lease connections from the `ConnectionPool` are responsible
        /// for removing the specific handlers they added to the `Channel` pipeline before releasing it to the pool.
        let channel: Channel

        /// Wether the connection is currently leased or not
        var isLeased: Bool = false

        /// Indicates that this connection is about to close
        var isClosing: Bool = false

        /// Convenience property indicating wether the underlying `Channel` is active or not
        var isActiveEstimation: Bool {
            return self.channel.isActive
        }
    }

    /// A connection provider of `HTTP/1.1` connections to a given `Key` (host, scheme, port)
    ///
    /// On top of enabling connection reuse this provider it also facilitates the creation
    /// of concurrent requests as it has built-in politness regarding the maximum number
    /// of concurrent requests to the server.
    class HTTP1ConnectionProvider {
        /// The default `EventLoop` for this provider
        ///
        /// The default event loop is used to create futures and is used
        /// when creating `Channel`s for requests for which the
        /// `EventLoopPreference` is set to `.indifferent`
        let eventLoop: EventLoop

        /// The client configuration used to bootstrap new requests
        private let configuration: HTTPClient.Configuration

        /// The key associated with this provider
        private let key: ConnectionPool.Key

        /// The `State` of this provider
        ///
        /// This property holds data structures representing the current state of the provider
        /// - Warning: This type isn't thread safe and should be accessed with proper
        /// synchronization (see the `stateLock` property)
        private var state: State

        /// The lock used to access and modify the `state` property
        private let stateLock = Lock()

        /// The maximum number of concurrent connections to a given (host, scheme, port)
        private let maximumConcurrentConnections: Int = 8

        /// The pool this provider belongs to
        private let parentPool: ConnectionPool

        /// Creates a new `HTTP1ConnectionProvider`
        ///
        /// - parameters:
        ///     - key: The `Key` (host, scheme, port) this provider is associated to
        ///     - configuration: The client configuration used globally by all requests
        ///     - initialConnection: The initial connection the pool initializes this provider with
        ///     - parentPool: The pool this provider belongs to
        init(key: ConnectionPool.Key, eventLoop: EventLoop, configuration: HTTPClient.Configuration, parentPool: ConnectionPool) {
            self.eventLoop = eventLoop
            self.configuration = configuration
            self.key = key
            self.parentPool = parentPool
            self.state = State(eventLoop: eventLoop, parentPool: parentPool, key: key)
        }

        deinit {
            self.stateLock.withLock {
                assert(self.state.availableConnections.isEmpty, "Available connections should be empty before deinit")
                assert(self.state.leased == 0, "All leased connections should have been returned before deinit \(ObjectIdentifier(self)) but there remains \(self.state.leased) \(Thread.current)")
            }
        }

        func getConnection(preference: HTTPClient.EventLoopPreference) -> EventLoopFuture<Connection> {
            self.preconditionIsOpened()
            let action = self.stateLock.withLock { self.state.connectionAction(for: preference) }
            switch action {
            case .leaseConnection(let connection):
                return self.eventLoop.makeSucceededFuture(connection)
            case .makeConnection(let eventLoop):
                return self.makeConnection(on: eventLoop)
            case .leaseFutureConnection(let futureConnection):
                return futureConnection
            }
        }

        func release(connection: Connection) {
            self.preconditionIsOpened()
            let action = self.stateLock.withLock { self.state.releaseAction(for: connection) }
            switch action {
            case .succeed(let promise):
                promise.succeed(connection)

            case .makeConnectionAndComplete(let eventLoop, let promise):
                self.makeConnection(on: eventLoop).cascade(to: promise)

            case .replaceConnection(let eventLoop):
                connection.channel.close().flatMap {
                    self.makeConnection(on: eventLoop)
                }.whenComplete { result in
                    switch result {
                    case .success(let connection):
                        self.release(connection: connection)
                    case .failure(let error):
                        let (promise, providerMustClose) = self.stateLock.withLock { self.state.popConnectionPromiseToFail() }
                        if providerMustClose {
                            self.stateLock.withLock {
                                self.state.removeFromPool()
                            }
                        }
                        promise?.fail(error)
                    }
                }

            case .none:
                break
            }
        }

        private func makeConnection(on eventLoop: EventLoop) -> EventLoopFuture<Connection> {
            self.preconditionIsOpened()
            let bootstrap = ClientBootstrap.makeHTTPClientBootstrapBase(group: eventLoop, host: self.key.host, port: self.key.port, configuration: self.configuration) { channel in
                channel.pipeline.addSSLHandlerIfNeeded(for: self.key, tlsConfiguration: self.configuration.tlsConfiguration).flatMap {
                    channel.pipeline.addHTTPClientHandlers(leftOverBytesStrategy: .forwardBytes)
                }
            }
            let address = HTTPClient.resolveAddress(host: self.key.host, port: self.key.port, proxy: self.configuration.proxy)
            return bootstrap.connect(host: address.host, port: address.port).map { channel in
                let connection = Connection(key: self.key, channel: channel, parentPool: self.parentPool)
                self.configureCloseCallback(of: connection)
                connection.isLeased = true
                return connection
            }
        }

        /// Adds a callback on connection close that asks the `state` what to do about this
        ///
        /// The callback informs the state about the event, and the state returns a
        /// `ClosedConnectionRemoveAction` which instructs it about what it should do.
        private func configureCloseCallback(of connection: Connection) {
            connection.channel.closeFuture.whenComplete { result in
                switch result {
                case .success:
                    let action = self.stateLock.withLock { self.state.removeClosedConnection(connection) }
                    switch action {
                    case .none:
                        break
                    case .makeConnectionAndComplete(let el, let promise):
                        self.makeConnection(on: el).cascade(to: promise)
                    }

                case .failure(let error):
                    preconditionFailure("Connection close future failed with error: \(error)")
                }
            }
        }

        func syncClose(requiresCleanClose: Bool) throws {
            let availableConnections = try self.stateLock.withLock { () -> CircularBuffer<ConnectionPool.Connection> in
                self.state.isClosed = true
                if requiresCleanClose {
                    guard self.state.leased == 0 else {
                        throw HTTPClientError.uncleanShutdown
                    }
                }
                return self.state.availableConnections
            }
            do {
                try EventLoopFuture<Void>.andAllComplete(availableConnections.map { $0.channel.close() }, on: self.eventLoop).wait()
            } catch ChannelError.alreadyClosed {
                return
            } catch {
                throw error
            }
            self.stateLock.withLock { self.state.availableConnections.removeAll() }
        }

        private func preconditionIsOpened() {
            self.stateLock.withLock {
                precondition(self.state.isClosed == false, "Attempting to use closed HTTP1ConnectionProvider")
            }
        }

        private struct State {
            /// The default `EventLoop` to use for this `HTTP1ConnectionProvider`
            private let defaultEventLoop: EventLoop

            /// The maximum number of connections to a certain (host, scheme, port) tuple.
            private let maximumConcurrentConnections: Int = 8

            /// Opened connections that are available
            fileprivate var availableConnections: CircularBuffer<Connection> = .init(initialCapacity: 8)

            /// The number of currently leased connections
            fileprivate var leased: Int = 0

            /// Consumers that weren't able to get a new connection without exceeding
            /// `maximumConcurrentConnections` get a `Future<Connection>`
            /// whose associated promise is stored in `Waiter`. The promise is completed
            /// as soon as possible by the provider, in FIFO order.
            private var waiters: CircularBuffer<Waiter> = .init(initialCapacity: 8)

            fileprivate var isClosed: Bool = false

            private let parentPool: ConnectionPool

            private let key: Key

            fileprivate init(eventLoop: EventLoop, parentPool: ConnectionPool, key: Key) {
                self.defaultEventLoop = eventLoop
                self.parentPool = parentPool
                self.key = key
            }

            fileprivate mutating func connectionAction(for preference: HTTPClient.EventLoopPreference) -> ConnectionGetAction {
                if self.leased < self.maximumConcurrentConnections {
                    self.leased += 1
                    let (channelEL, requiresSpecifiedEL) = self.resolvePreference(preference)

                    if let connection = availableConnections.swapRemove(where: { $0.channel.eventLoop === channelEL }) {
                        connection.isLeased = true
                        return .leaseConnection(connection)
                    } else {
                        if requiresSpecifiedEL {
                            return .makeConnection(channelEL)
                        } else if let existingConnection = availableConnections.popFirst() {
                            return .leaseConnection(existingConnection)
                        } else {
                            return .makeConnection(self.defaultEventLoop)
                        }
                    }
                } else {
                    let promise = self.defaultEventLoop.makePromise(of: Connection.self)
                    self.waiters.append(Waiter(promise: promise, preference: preference))
                    return .leaseFutureConnection(promise.futureResult)
                }
            }

            fileprivate mutating func releaseAction(for connection: Connection) -> ConnectionReleaseAction {
                if let firstWaiter = waiters.first {
                    let (channelEL, requiresSpecifiedEL) = self.resolvePreference(firstWaiter.preference)

                    guard connection.isActiveEstimation, !connection.isClosing else {
                        return .makeConnectionAndComplete(channelEL, firstWaiter.promise)
                    }

                    if connection.channel.eventLoop === channelEL {
                        self.waiters.removeFirst()
                        return .succeed(firstWaiter.promise)
                    } else {
                        if requiresSpecifiedEL {
                            self.leased -= 1
                            return .replaceConnection(channelEL)
                        } else {
                            return .makeConnectionAndComplete(channelEL, firstWaiter.promise)
                        }
                    }

                } else {
                    connection.isLeased = false
                    self.leased -= 1
                    if connection.isActiveEstimation, !connection.isClosing {
                        self.availableConnections.append(connection)
                    }

                    if self.providerMustClose() {
                        self.removeFromPool()
                    }
                    
                    return .none
                }
            }

            fileprivate mutating func removeClosedConnection(_ connection: Connection) -> ClosedConnectionRemoveAction {
                if connection.isLeased {
                    if let firstWaiter = self.waiters.popFirst() {
                        let (el, _) = self.resolvePreference(firstWaiter.preference)
                        return .makeConnectionAndComplete(el, firstWaiter.promise)
                    }
                } else {
                    self.availableConnections.swapRemove(where: { $0 === connection })
                }
                
                if self.providerMustClose() {
                    self.removeFromPool()
                }
                
                return .none
            }

            fileprivate mutating func popConnectionPromiseToFail() -> (promise: EventLoopPromise<Connection>?, providerMustClose: Bool) {
                return (self.waiters.popFirst()?.promise, self.providerMustClose())
            }

            private func providerMustClose() -> Bool {
                return self.parentPool.connectionProvidersLock.withLock {
                    !self.isClosed && self.leased == 0 && self.availableConnections.isEmpty && self.waiters.isEmpty
                }
            }

            fileprivate mutating func removeFromPool() {
                self.parentPool[self.key] = nil
                self.isClosed = true
            }

            private func resolvePreference(_ preference: HTTPClient.EventLoopPreference) -> (EventLoop, Bool) {
                switch preference.preference {
                case .indifferent:
                    return (self.defaultEventLoop, false)
                case .delegate(let el):
                    return (el, false)
                case .delegateAndChannel(let el), .testOnly_exact(let el, _):
                    return (el, true)
                }
            }

            fileprivate enum ConnectionGetAction {
                case leaseConnection(Connection)
                case makeConnection(EventLoop)
                case leaseFutureConnection(EventLoopFuture<Connection>)
            }

            fileprivate enum ConnectionReleaseAction {
                case succeed(EventLoopPromise<Connection>)
                case makeConnectionAndComplete(EventLoop, EventLoopPromise<Connection>)
                case replaceConnection(EventLoop)
                case none
            }

            fileprivate enum ClosedConnectionRemoveAction {
                case none
                case makeConnectionAndComplete(EventLoop, EventLoopPromise<Connection>)
            }

            /// A `Waiter` represents a request that waits for a connection when none is
            /// currently available
            ///
            /// `Waiter`s are created when `maximumConcurrentConnections` is reached
            /// and we cannot create new connections anymore.
            private struct Waiter {
                /// The promise to complete once a connection is available
                let promise: EventLoopPromise<Connection>

                /// The event loop preference associated to this particular request
                /// that the provider should respect
                let preference: HTTPClient.EventLoopPreference
            }
        }
    }
}
