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
    var _connectionProviders: [Key: ConnectionProvider] = [:]

    /// The lock used by the connection pool used to ensure correct synchronization of accesses to `_connectionProviders`
    ///
    /// Accesses that only need synchronization for the span of a get or set can use the subscript instead of this property
    private let connectionProvidersLock = Lock()

    init(configuration: HTTPClient.Configuration) {
        self.configuration = configuration
    }

    /// This enables the connection pool to store and retreive `ConnectionProvider`s
    /// by ensuring thread safety using the `connectionProvidersLock`.
    private subscript(key: Key) -> ConnectionProvider? {
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

    /// Determines what action should be taken regarding the `ConnectionProvider`
    ///
    /// This method ensures there is no race condition if called concurrently from multiple threads
    private func getAction(for key: Key, on eventLoop: EventLoop) -> ProviderGetAction {
        return self.connectionProvidersLock.withLock {
            if let provider = self._connectionProviders[key] {
                return .useExisting(provider)
            } else {
                let promise = eventLoop.makePromise(of: ConnectionProvider.self)
                self._connectionProviders[key] = .future(promise.futureResult)
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
    func getConnection(for request: HTTPClient.Request, preference: HTTPClient.EventLoopPreference, on eventLoop: EventLoop, deadline: NIODeadline?) -> EventLoopFuture<Connection> {
        let key = Key(request)
        switch self.getAction(for: key, on: eventLoop) {
        case .useExisting(let provider):
            return provider.getConnection(preference: preference)

        case .make(completing: let providerPromise):
            // If no connection provider exists for the given key, create a new one
            let selectedEventLoop = providerPromise.futureResult.eventLoop
            var bootstrap = ClientBootstrap.makeHTTPClientBootstrapBase(group: selectedEventLoop, host: request.host, port: request.port, configuration: self.configuration)

            if let timeout = resolve(timeout: self.configuration.timeout.connect, deadline: deadline) {
                bootstrap = bootstrap.connectTimeout(timeout)
            }

            let address = HTTPClient.resolveAddress(host: request.host, port: request.port, proxy: self.configuration.proxy)

            return bootstrap.connect(host: address.host, port: address.port).flatMap { channel in
                channel.pipeline.addSSLHandlerIfNeeded(for: key, tlsConfiguration: self.configuration.tlsConfiguration).flatMap {
                    channel.pipeline.addHTTPClientHandlers(leftOverBytesStrategy: .forwardBytes)
                }.flatMap {
                    let provider = ConnectionProvider.http1(HTTP1ConnectionProvider(key: key, configuration: self.configuration, initialConnection: Connection(key: key, channel: channel, parentPool: self), parentPool: self))
                    providerPromise.succeed(provider)
                    return provider.getConnection(preference: preference)
                }
            }
        }
    }

    func release(_ connection: Connection) {
        self.connectionProvidersLock.withLock {
            if let provider = self._connectionProviders[connection.key] {
                provider.release(connection: connection)
            }
        }
    }

    func syncClose(requiresCleanClose: Bool) throws {
        let connectionProviders = self.connectionProvidersLock.withLock { _connectionProviders.values }
        for connectionProvider in connectionProviders {
            try connectionProvider.syncClose(requiresCleanClose: requiresCleanClose)
        }
    }

    /// Used by the `ConnectionPool` to index its `ConnectionProvider`s
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
    /// In the `ConnectionPool`, each `Channel` belongs to a given `ConnectionProvider`
    /// and has a certain "lease state" (see the `isLeased` property).
    /// The role of `Connection` is to model this by storing a `Channel` alongside its associated properties
    /// so that they can be passed around together.
    class Connection {
        init(key: Key, channel: Channel, parentPool: ConnectionPool) {
            self.key = key
            self.channel = channel
            self.parentPool = parentPool
        }

        /// Release this `Connection` to its associated `ConnectionProvider` in the parent `ConnectionPool`
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

        /// The `Key` of this `ConnectionProvider` this `Connection` belongs to
        ///
        /// This lets `ConnectionPool` know the relationship between `Connection`s and `ConnectionProvider`s
        fileprivate let key: Key

        /// The `Channel` of this `Connection`
        ///
        /// - Warning: Requests that lease connections from the `ConnectionPool` are responsible
        /// for removing the specific handlers they added to the `Channel` pipeline before releasing it to the pool.
        let channel: Channel

        /// Wether the connection is currently leased or not
        var isLeased: NIOAtomic<Bool> = .makeAtomic(value: false)

        /// Convenience property indicating wether the underlying `Channel` is active or not
        var isActive: Bool {
            return self.channel.isActive
        }
    }

    /// Abstracts the underlying kind of connection provider
    ///
    /// Connection providers are responsible for creating, storing and leasing all connections
    /// to a given (host, scheme, port) tuple, that is represented by the `Key` type.
    enum ConnectionProvider {
        /// An HTTP/1.1 connection provider
        case http1(HTTP1ConnectionProvider)

        /// A future connection provider
        ///
        /// This case lets us indicate that a `ConnectionProvider` has already been
        /// requested for the `Key` (ie: the `(host, scheme, port)`) of the request.
        /// This prevents multiple subsequent connections associated to a given `Key` from each
        /// trying to create its own connection provider when there is none available yet.
        ///
        /// This makes the `ConnectionProvider` creation state  completely opaque to the callers
        /// of methods such as  `getConnection(...)`, `release(...)` and `syncClose(...)`,
        /// which has the benefit of making the logic much easier on their side, by just relying on the fact that
        /// the call will *eventually* be passed to the `ConnectionProvider` when it becomes available.
        indirect case future(EventLoopFuture<ConnectionProvider>)

        /// Request a connection from this provider that conforms to the specified `preference`.
        func getConnection(preference: HTTPClient.EventLoopPreference) -> EventLoopFuture<Connection> {
            switch self {
            case .http1(let provider):
                return provider.getConnection(preference: preference)
            case .future(let futureProvider):
                return futureProvider.flatMap { provider in
                    provider.getConnection(preference: preference)
                }
            }
        }

        /// Release a connection to this provider
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

        /// Close the provider
        ///
        /// This closes all stored connections for this provider, optionally making some internal
        /// consistency checks (see `HTTPClient.syncShutdown(requiresCleanClose:)`)
        /// A provider *must not* be used once it has been closed.
        ///
        /// - parameters:
        ///     - requiresCleanClose: Makes additional consistency checks on close
        func syncClose(requiresCleanClose: Bool) throws {
            switch self {
            case .http1(let provider):
                try provider.syncClose(requiresCleanClose: requiresCleanClose)
            case .future(let futureProvider):
                try futureProvider.wait().syncClose(requiresCleanClose: requiresCleanClose)
            }
        }

        /// Each `ConnectionProvider` has an associated, default `EventLoop`
        ///
        /// See `ConnectionPool.associatedEventLoop(for key: Key)` for more details
        var eventLoop: EventLoop {
            switch self {
            case .future(let future):
                return future.eventLoop
            case .http1(let provider):
                return provider.eventLoop
            }
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

        /// Wether this provider is closed or not
        private var isClosed: NIOAtomic<Bool>

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
        init(key: ConnectionPool.Key, configuration: HTTPClient.Configuration, initialConnection: Connection, parentPool: ConnectionPool) {
            self.eventLoop = initialConnection.channel.eventLoop
            self.configuration = configuration
            self.key = key
            self.parentPool = parentPool
            self.isClosed = NIOAtomic.makeAtomic(value: false)
            self.state = State(initialConnection: initialConnection)
            self.configureCloseCallback(of: initialConnection)
        }

        deinit {
            // FIXME: Add the checks back
            /*
             assert(availableConnections.isEmpty, "Available connections should be empty before deinit")
             assert(leased == 0, "All leased connections should have been returned before deinit")
             */
        }

        func getConnection(preference: HTTPClient.EventLoopPreference) -> EventLoopFuture<Connection> {
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
                        promise?.fail(error)
                        if providerMustClose {
                            self.parentPool[self.key] = nil
                        }
                    }
                }

            case .removeProvider:
                // FIXME: Need to close the pool
                break

            case .none:
                break
            }
        }

        private func makeConnection(on eventLoop: EventLoop) -> EventLoopFuture<Connection> {
            let bootstrap = ClientBootstrap.makeHTTPClientBootstrapBase(group: eventLoop, host: self.key.host, port: self.key.port, configuration: self.configuration) { channel in
                channel.pipeline.addSSLHandlerIfNeeded(for: self.key, tlsConfiguration: self.configuration.tlsConfiguration).flatMap {
                    channel.pipeline.addHTTPClientHandlers(leftOverBytesStrategy: .forwardBytes)
                }
            }
            let address = HTTPClient.resolveAddress(host: self.key.host, port: self.key.port, proxy: self.configuration.proxy)
            return bootstrap.connect(host: address.host, port: address.port).map { channel in
                let connection = Connection(key: self.key, channel: channel, parentPool: self.parentPool)
                self.configureCloseCallback(of: connection)
                connection.isLeased.store(true)
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
                    case .removeProvider:
                        self.parentPool[self.key] = nil
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

            fileprivate init(initialConnection: Connection) {
                self.availableConnections.append(initialConnection)
                self.defaultEventLoop = initialConnection.channel.eventLoop
            }

            fileprivate mutating func connectionAction(for preference: HTTPClient.EventLoopPreference) -> ConnectionGetAction {
                if self.leased < self.maximumConcurrentConnections {
                    self.leased += 1
                    let (channelEL, requiresSpecifiedEL) = self.resolvePreference(preference)

                    if let connection = availableConnections.swapRemove(where: { $0.channel.eventLoop === channelEL }) {
                        connection.isLeased.store(true)
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

                    guard connection.isActive else {
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
                    self.leased -= 1
                    if connection.isActive {
                        self.availableConnections.append(connection)
                        connection.isLeased.store(false)
                    }
                    return self.providerMustClose() ? .removeProvider : .none
                }
            }

            fileprivate mutating func removeClosedConnection(_ connection: Connection) -> ClosedConnectionRemoveAction {
                if connection.isLeased.load() {
                    self.leased -= 1
                    if let firstWaiter = self.waiters.popFirst() {
                        let (el, _) = self.resolvePreference(firstWaiter.preference)
                        return .makeConnectionAndComplete(el, firstWaiter.promise)
                    }
                } else {
                    self.availableConnections.swapRemove(where: { $0 === connection })
                }
                return self.providerMustClose() ? .removeProvider : .none
            }

            fileprivate mutating func popConnectionPromiseToFail() -> (promise: EventLoopPromise<Connection>?, providerMustClose: Bool) {
                return (self.waiters.popFirst()?.promise, self.providerMustClose())
            }

            private func providerMustClose() -> Bool {
                return self.leased == 0 && self.availableConnections.isEmpty && self.waiters.isEmpty
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
                case removeProvider
                case none
            }

            fileprivate enum ClosedConnectionRemoveAction {
                case none
                case removeProvider
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

    private enum ProviderGetAction {
        case make(EventLoopPromise<ConnectionProvider>)
        case useExisting(ConnectionProvider)
    }
}
