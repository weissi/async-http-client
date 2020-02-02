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

import AsyncHTTPClient
import NIO
import NIOFoundationCompat
import NIOHTTP1
import NIOHTTPCompression
import NIOSSL
import NIOTestUtils
import XCTest

class HTTPClientTests: XCTestCase {
    typealias Request = HTTPClient.Request

    var group: EventLoopGroup!

    override func setUp() {
        XCTAssertNil(self.group)
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    override func tearDown() {
        XCTAssertNotNil(self.group)
        XCTAssertNoThrow(try self.group.syncShutdownGracefully())
        self.group = nil
    }

    func testRequestURI() throws {
        let request1 = try Request(url: "https://someserver.com:8888/some/path?foo=bar")
        XCTAssertEqual(request1.url.host, "someserver.com")
        XCTAssertEqual(request1.url.path, "/some/path")
        XCTAssertEqual(request1.url.query!, "foo=bar")
        XCTAssertEqual(request1.port, 8888)
        XCTAssertTrue(request1.useTLS)

        let request2 = try Request(url: "https://someserver.com")
        XCTAssertEqual(request2.url.path, "")
    }

    func testBadRequestURI() throws {
        XCTAssertThrowsError(try Request(url: "some/path"), "should throw") { error in
            XCTAssertEqual(error as! HTTPClientError, HTTPClientError.emptyScheme)
        }
        XCTAssertThrowsError(try Request(url: "file://somewhere/some/path?foo=bar"), "should throw") { error in
            XCTAssertEqual(error as! HTTPClientError, HTTPClientError.unsupportedScheme("file"))
        }
        XCTAssertThrowsError(try Request(url: "https:/foo"), "should throw") { error in
            XCTAssertEqual(error as! HTTPClientError, HTTPClientError.emptyHost)
        }
    }

    func testSchemaCasing() throws {
        XCTAssertNoThrow(try Request(url: "hTTpS://someserver.com:8888/some/path?foo=bar"))
    }

    func testGet() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let response = try httpClient.get(url: "http://localhost:\(httpBin.port)/get").wait()
        XCTAssertEqual(.ok, response.status)
    }

    func testGetWithDifferentEventLoopBackpressure() throws {
        let httpBin = HTTPBin()
        let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let external = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(loopGroup))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try loopGroup.syncShutdownGracefully())
            XCTAssertNoThrow(try httpBin.shutdown())
        }
        let request = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/events/10/1")
        let delegate = TestHTTPDelegate(backpressureEventLoop: external.next())
        let task = httpClient.execute(request: request, delegate: delegate)
        try task.wait()
    }

    func testPost() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let response = try httpClient.post(url: "http://localhost:\(httpBin.port)/post", body: .string("1234")).wait()
        let bytes = response.body.flatMap { $0.getData(at: 0, length: $0.readableBytes) }
        let data = try JSONDecoder().decode(RequestInfo.self, from: bytes!)

        XCTAssertEqual(.ok, response.status)
        XCTAssertEqual("1234", data.data)
    }

    func testGetHttps() throws {
        let httpBin = HTTPBin(ssl: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let response = try httpClient.get(url: "https://localhost:\(httpBin.port)/get").wait()
        XCTAssertEqual(.ok, response.status)
    }

    func testGetHttpsWithIP() throws {
        let httpBin = HTTPBin(ssl: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let response = try httpClient.get(url: "https://127.0.0.1:\(httpBin.port)/get").wait()
        XCTAssertEqual(.ok, response.status)
    }

    func testPostHttps() throws {
        let httpBin = HTTPBin(ssl: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let request = try Request(url: "https://localhost:\(httpBin.port)/post", method: .POST, body: .string("1234"))

        let response = try httpClient.execute(request: request).wait()
        let bytes = response.body.flatMap { $0.getData(at: 0, length: $0.readableBytes) }
        let data = try JSONDecoder().decode(RequestInfo.self, from: bytes!)

        XCTAssertEqual(.ok, response.status)
        XCTAssertEqual("1234", data.data)
    }

    func testHttpRedirect() throws {
        let httpBin = HTTPBin(ssl: false)
        let httpsBin = HTTPBin(ssl: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none, redirectConfiguration: .follow(max: 10, allowCycles: true)))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
            XCTAssertNoThrow(try httpsBin.shutdown())
        }

        var response = try httpClient.get(url: "http://localhost:\(httpBin.port)/redirect/302").wait()
        XCTAssertEqual(response.status, .ok)

        response = try httpClient.get(url: "http://localhost:\(httpBin.port)/redirect/https?port=\(httpsBin.port)").wait()
        XCTAssertEqual(response.status, .ok)
    }

    func testHttpHostRedirect() throws {
        let httpBin = HTTPBin(ssl: false)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none, redirectConfiguration: .follow(max: 10, allowCycles: true)))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let response = try httpClient.get(url: "http://localhost:\(httpBin.port)/redirect/loopback?port=\(httpBin.port)").wait()
        guard var body = response.body else {
            XCTFail("The target page should have a body containing the value of the Host header")
            return
        }
        guard let responseData = body.readData(length: body.readableBytes) else {
            XCTFail("Read data shouldn't be nil since we passed body.readableBytes to body.readData")
            return
        }
        let decoder = JSONDecoder()
        let hostName = try decoder.decode([String: String].self, from: responseData)["data"]
        XCTAssert(hostName == "127.0.0.1")
    }

    func testPercentEncoded() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let response = try httpClient.get(url: "http://localhost:\(httpBin.port)/percent%20encoded").wait()
        XCTAssertEqual(.ok, response.status)
    }

    func testMultipleContentLengthHeaders() throws {
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
        }
        let httpBin = HTTPBin()
        defer {
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let body = ByteBuffer.of(string: "hello world!")

        var headers = HTTPHeaders()
        headers.add(name: "Content-Length", value: "12")
        let request = try Request(url: "http://localhost:\(httpBin.port)/post", method: .POST, headers: headers, body: .byteBuffer(body))
        let response = try httpClient.execute(request: request).wait()
        // if the library adds another content length header we'll get a bad request error.
        XCTAssertEqual(.ok, response.status)
    }

    func testStreaming() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        var request = try Request(url: "http://localhost:\(httpBin.port)/events/10/1")
        request.headers.add(name: "Accept", value: "text/event-stream")

        let delegate = CountingDelegate()
        let count = try httpClient.execute(request: request, delegate: delegate).wait()

        XCTAssertEqual(10, count)
    }

    func testRemoteClose() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        XCTAssertThrowsError(try httpClient.get(url: "http://localhost:\(httpBin.port)/close").wait(), "Should fail") { error in
            guard case let error = error as? HTTPClientError, error == .remoteConnectionClosed else {
                return XCTFail("Should fail with remoteConnectionClosed")
            }
        }
    }

    func testReadTimeout() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew, configuration: HTTPClient.Configuration(timeout: HTTPClient.Configuration.Timeout(read: .milliseconds(150))))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        XCTAssertThrowsError(try httpClient.get(url: "http://localhost:\(httpBin.port)/wait").wait(), "Should fail") { error in
            guard case let error = error as? HTTPClientError, error == .readTimeout else {
                return XCTFail("Should fail with readTimeout")
            }
        }
    }

    func testDeadline() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        XCTAssertThrowsError(try httpClient.get(url: "http://localhost:\(httpBin.port)/wait", deadline: .now() + .milliseconds(150)).wait(), "Should fail") { error in
            guard case let error = error as? HTTPClientError, error == .readTimeout else {
                return XCTFail("Should fail with readTimeout")
            }
        }
    }

    func testCancel() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let queue = DispatchQueue(label: "nio-test")
        let request = try Request(url: "http://localhost:\(httpBin.port)/wait")
        let task = httpClient.execute(request: request, delegate: TestHTTPDelegate())

        queue.asyncAfter(deadline: .now() + .milliseconds(100)) {
            task.cancel()
        }

        XCTAssertThrowsError(try task.wait(), "Should fail") { error in
            guard case let error = error as? HTTPClientError, error == .cancelled else {
                return XCTFail("Should fail with cancelled")
            }
        }
    }

    func testStressCancel() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let request = try Request(url: "http://localhost:\(httpBin.port)/wait", method: .GET)
        let tasks = (1...100).map { _ -> HTTPClient.Task<TestHTTPDelegate.Response> in
            let task = httpClient.execute(request: request, delegate: TestHTTPDelegate())
            task.cancel()
            return task
        }

        for task in tasks {
            switch (Result { try task.futureResult.timeout(after: .seconds(10)).wait() }) {
            case .success:
                XCTFail("Shouldn't succeed")
                return
            case .failure(let error):
                guard let clientError = error as? HTTPClientError, clientError == .cancelled else {
                    XCTFail("Unexpected error: \(error)")
                    return
                }
            }
        }
    }

    func testHTTPClientAuthorization() {
        var authorization = HTTPClient.Authorization.basic(username: "aladdin", password: "opensesame")
        XCTAssertEqual(authorization.headerValue, "Basic YWxhZGRpbjpvcGVuc2VzYW1l")

        authorization = HTTPClient.Authorization.bearer(tokens: "mF_9.B5f-4.1JqM")
        XCTAssertEqual(authorization.headerValue, "Bearer mF_9.B5f-4.1JqM")
    }

    func testProxyPlaintext() throws {
        let httpBin = HTTPBin(simulateProxy: .plaintext)
        let httpClient = HTTPClient(
            eventLoopGroupProvider: .createNew,
            configuration: .init(proxy: .server(host: "localhost", port: httpBin.port))
        )
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }
        let res = try httpClient.get(url: "http://test/ok").wait()
        XCTAssertEqual(res.status, .ok)
    }

    func testProxyTLS() throws {
        let httpBin = HTTPBin(simulateProxy: .tls)
        let httpClient = HTTPClient(
            eventLoopGroupProvider: .createNew,
            configuration: .init(
                certificateVerification: .none,
                proxy: .server(host: "localhost", port: httpBin.port)
            )
        )
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }
        let res = try httpClient.get(url: "https://test/ok").wait()
        XCTAssertEqual(res.status, .ok)
    }

    func testProxyPlaintextWithCorrectlyAuthorization() throws {
        let httpBin = HTTPBin(simulateProxy: .plaintext)
        let httpClient = HTTPClient(
            eventLoopGroupProvider: .createNew,
            configuration: .init(proxy: .server(host: "localhost", port: httpBin.port, authorization: .basic(username: "aladdin", password: "opensesame")))
        )
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }
        let res = try httpClient.get(url: "http://test/ok").wait()
        XCTAssertEqual(res.status, .ok)
    }

    func testProxyPlaintextWithIncorrectlyAuthorization() throws {
        let httpBin = HTTPBin(simulateProxy: .plaintext)
        let httpClient = HTTPClient(
            eventLoopGroupProvider: .createNew,
            configuration: .init(proxy: .server(host: "localhost", port: httpBin.port, authorization: .basic(username: "aladdin", password: "opensesamefoo")))
        )
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }
        XCTAssertThrowsError(try httpClient.get(url: "http://test/ok").wait(), "Should fail") { error in
            guard case let error = error as? HTTPClientError, error == .proxyAuthenticationRequired else {
                return XCTFail("Should fail with HTTPClientError.proxyAuthenticationRequired")
            }
        }
    }

    func testUploadStreaming() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let body: HTTPClient.Body = .stream(length: 8) { writer in
            let buffer = ByteBuffer.of(string: "1234")
            return writer.write(.byteBuffer(buffer)).flatMap {
                let buffer = ByteBuffer.of(string: "4321")
                return writer.write(.byteBuffer(buffer))
            }
        }

        let response = try httpClient.post(url: "http://localhost:\(httpBin.port)/post", body: body).wait()
        let bytes = response.body.flatMap { $0.getData(at: 0, length: $0.readableBytes) }
        let data = try JSONDecoder().decode(RequestInfo.self, from: bytes!)

        XCTAssertEqual(.ok, response.status)
        XCTAssertEqual("12344321", data.data)
    }

    func testNoContentLengthForSSLUncleanShutdown() throws {
        let httpBin = HttpBinForSSLUncleanShutdown()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            httpBin.shutdown()
        }

        XCTAssertThrowsError(try httpClient.get(url: "https://localhost:\(httpBin.port)/nocontentlength").wait(), "Should fail") { error in
            guard case let error = error as? NIOSSLError, error == .uncleanShutdown else {
                return XCTFail("Should fail with NIOSSLError.uncleanShutdown")
            }
        }
    }

    func testNoContentLengthWithIgnoreErrorForSSLUncleanShutdown() throws {
        let httpBin = HttpBinForSSLUncleanShutdown()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none, ignoreUncleanSSLShutdown: true))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            httpBin.shutdown()
        }

        let response = try httpClient.get(url: "https://localhost:\(httpBin.port)/nocontentlength").wait()
        let bytes = response.body.flatMap { $0.getData(at: 0, length: $0.readableBytes) }
        let string = String(decoding: bytes!, as: UTF8.self)

        XCTAssertEqual(.ok, response.status)
        XCTAssertEqual("foo", string)
    }

    func testCorrectContentLengthForSSLUncleanShutdown() throws {
        let httpBin = HttpBinForSSLUncleanShutdown()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            httpBin.shutdown()
        }

        let response = try httpClient.get(url: "https://localhost:\(httpBin.port)/").wait()
        let bytes = response.body.flatMap { $0.getData(at: 0, length: $0.readableBytes) }
        let string = String(decoding: bytes!, as: UTF8.self)

        XCTAssertEqual(.notFound, response.status)
        XCTAssertEqual("Not Found", string)
    }

    func testNoContentForSSLUncleanShutdown() throws {
        let httpBin = HttpBinForSSLUncleanShutdown()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            httpBin.shutdown()
        }

        let response = try httpClient.get(url: "https://localhost:\(httpBin.port)/nocontent").wait()

        XCTAssertEqual(.noContent, response.status)
        XCTAssertEqual(response.body, nil)
    }

    func testNoResponseForSSLUncleanShutdown() throws {
        let httpBin = HttpBinForSSLUncleanShutdown()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            httpBin.shutdown()
        }

        XCTAssertThrowsError(try httpClient.get(url: "https://localhost:\(httpBin.port)/noresponse").wait(), "Should fail") { error in
            guard case let error = error as? NIOSSLError, error == .uncleanShutdown else {
                return XCTFail("Should fail with NIOSSLError.uncleanShutdown")
            }
        }
    }

    func testNoResponseWithIgnoreErrorForSSLUncleanShutdown() throws {
        let httpBin = HttpBinForSSLUncleanShutdown()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none, ignoreUncleanSSLShutdown: true))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            httpBin.shutdown()
        }

        XCTAssertThrowsError(try httpClient.get(url: "https://localhost:\(httpBin.port)/noresponse").wait(), "Should fail") { error in
            guard case let error = error as? NIOSSLError, error == .uncleanShutdown else {
                return XCTFail("Should fail with NIOSSLError.uncleanShutdown")
            }
        }
    }

    func testWrongContentLengthForSSLUncleanShutdown() throws {
        let httpBin = HttpBinForSSLUncleanShutdown()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            httpBin.shutdown()
        }

        XCTAssertThrowsError(try httpClient.get(url: "https://localhost:\(httpBin.port)/wrongcontentlength").wait(), "Should fail") { error in
            guard case let error = error as? NIOSSLError, error == .uncleanShutdown else {
                return XCTFail("Should fail with NIOSSLError.uncleanShutdown")
            }
        }
    }

    func testWrongContentLengthWithIgnoreErrorForSSLUncleanShutdown() throws {
        let httpBin = HttpBinForSSLUncleanShutdown()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none, ignoreUncleanSSLShutdown: true))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            httpBin.shutdown()
        }

        XCTAssertThrowsError(try httpClient.get(url: "https://localhost:\(httpBin.port)/wrongcontentlength").wait(), "Should fail") { error in
            guard case let error = error as? HTTPParserError, error == .invalidEOFState else {
                return XCTFail("Should fail with HTTPParserError.invalidEOFState")
            }
        }
    }

    func testEventLoopArgument() throws {
        let httpBin = HTTPBin()
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 5)
        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(eventLoopGroup),
                                    configuration: HTTPClient.Configuration(redirectConfiguration: .follow(max: 10, allowCycles: true)))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        class EventLoopValidatingDelegate: HTTPClientResponseDelegate {
            typealias Response = Bool

            let eventLoop: EventLoop
            var result = false

            init(eventLoop: EventLoop) {
                self.eventLoop = eventLoop
            }

            func didReceiveHead(task: HTTPClient.Task<Bool>, _ head: HTTPResponseHead) -> EventLoopFuture<Void> {
                self.result = task.eventLoop === self.eventLoop
                return task.eventLoop.makeSucceededFuture(())
            }

            func didFinishRequest(task: HTTPClient.Task<Bool>) throws -> Bool {
                return self.result
            }
        }

        let eventLoop = eventLoopGroup.next()
        let delegate = EventLoopValidatingDelegate(eventLoop: eventLoop)
        var request = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/get")
        var response = try httpClient.execute(request: request, delegate: delegate, eventLoop: .delegate(on: eventLoop)).wait()
        XCTAssertEqual(true, response)

        // redirect
        request = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/redirect/302")
        response = try httpClient.execute(request: request, delegate: delegate, eventLoop: .delegate(on: eventLoop)).wait()
        XCTAssertEqual(true, response)
    }

    func testDecompression() throws {
        let httpBin = HTTPBin(compress: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew, configuration: .init(decompression: .enabled(limit: .none)))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        var body = ""
        for _ in 1...1000 {
            body += "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
        }

        for algorithm in [nil, "gzip", "deflate"] {
            var request = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/post", method: .POST)
            request.body = .string(body)
            if let algorithm = algorithm {
                request.headers.add(name: "Accept-Encoding", value: algorithm)
            }

            let response = try httpClient.execute(request: request).wait()
            let bytes = response.body!.getData(at: 0, length: response.body!.readableBytes)!
            let data = try JSONDecoder().decode(RequestInfo.self, from: bytes)

            XCTAssertEqual(.ok, response.status)
            XCTAssertGreaterThan(body.count, response.headers["Content-Length"].first.flatMap { Int($0) }!)
            if let algorithm = algorithm {
                XCTAssertEqual(algorithm, response.headers["Content-Encoding"].first)
            } else {
                XCTAssertEqual("deflate", response.headers["Content-Encoding"].first)
            }
            XCTAssertEqual(body, data.data)
        }
    }

    func testDecompressionLimit() throws {
        let httpBin = HTTPBin(compress: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew, configuration: .init(decompression: .enabled(limit: .ratio(10))))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        var request = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/post", method: .POST)
        request.body = .byteBuffer(ByteBuffer.of(bytes: [120, 156, 75, 76, 28, 5, 200, 0, 0, 248, 66, 103, 17]))
        request.headers.add(name: "Accept-Encoding", value: "deflate")

        do {
            _ = try httpClient.execute(request: request).wait()
        } catch let error as NIOHTTPDecompression.DecompressionError {
            switch error {
            case .limit:
                // ok
                break
            default:
                XCTFail("Unexptected error: \(error)")
            }
        } catch {
            XCTFail("Unexptected error: \(error)")
        }
    }

    func testLoopDetectionRedirectLimit() throws {
        let httpBin = HTTPBin(ssl: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none, redirectConfiguration: .follow(max: 5, allowCycles: false)))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        XCTAssertThrowsError(try httpClient.get(url: "https://localhost:\(httpBin.port)/redirect/infinite1").wait(), "Should fail with redirect limit") { error in
            XCTAssertEqual(error as! HTTPClientError, HTTPClientError.redirectCycleDetected)
        }
    }

    func testCountRedirectLimit() throws {
        let httpBin = HTTPBin(ssl: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none, redirectConfiguration: .follow(max: 5, allowCycles: true)))

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        XCTAssertThrowsError(try httpClient.get(url: "https://localhost:\(httpBin.port)/redirect/infinite1").wait(), "Should fail with redirect limit") { error in
            XCTAssertEqual(error as! HTTPClientError, HTTPClientError.redirectLimitReached)
        }
    }

    func testMultipleConcurrentRequests() throws {
        let numberOfRequestsPerThread = 1000
        let numberOfParallelWorkers = 5

        final class HTTPServer: ChannelInboundHandler {
            typealias InboundIn = HTTPServerRequestPart
            typealias OutboundOut = HTTPServerResponsePart

            func channelRead(context: ChannelHandlerContext, data: NIOAny) {
                if case .end = self.unwrapInboundIn(data) {
                    let responseHead = HTTPServerResponsePart.head(.init(version: .init(major: 1, minor: 1),
                                                                         status: .ok))
                    context.write(self.wrapOutboundOut(responseHead), promise: nil)
                    context.writeAndFlush(self.wrapOutboundOut(.end(nil)), promise: nil)
                }
            }
        }

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        defer {
            XCTAssertNoThrow(try group.syncShutdownGracefully())
        }

        var server: Channel?
        XCTAssertNoThrow(server = try ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socket(.init(SOL_SOCKET), .init(SO_REUSEADDR)), value: 1)
            .serverChannelOption(ChannelOptions.backlog, value: .init(numberOfParallelWorkers))
            .childChannelInitializer { channel in
                channel.pipeline.configureHTTPServerPipeline(withPipeliningAssistance: false,
                                                             withServerUpgrade: nil,
                                                             withErrorHandling: false).flatMap {
                    channel.pipeline.addHandler(HTTPServer())
                }
            }
            .bind(to: .init(ipAddress: "127.0.0.1", port: 0))
            .wait())
        defer {
            XCTAssertNoThrow(try server?.close().wait())
        }

        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(group))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
        }

        let g = DispatchGroup()
        for workerID in 0..<numberOfParallelWorkers {
            DispatchQueue(label: "\(#file):\(#line):worker-\(workerID)").async(group: g) {
                func makeRequest() {
                    let url = "http://127.0.0.1:\(server?.localAddress?.port ?? -1)/hello"
                    XCTAssertNoThrow(try httpClient.get(url: url).wait())
                }
                for _ in 0..<numberOfRequestsPerThread {
                    makeRequest()
                }
            }
        }
        let timeout = DispatchTime.now() + .seconds(180)
        switch g.wait(timeout: timeout) {
        case .success:
            break
        case .timedOut:
            XCTFail("Timed out")
        }
    }

    func testWorksWith500Error() {
        let web = NIOHTTP1TestServer(group: self.group)
        defer {
            XCTAssertNoThrow(try web.stop())
        }

        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(self.group))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
        }
        let result = httpClient.get(url: "http://localhost:\(web.serverPort)/foo")

        XCTAssertNoThrow(XCTAssertEqual(.head(.init(version: .init(major: 1, minor: 1),
                                                    method: .GET,
                                                    uri: "/foo",
                                                    headers: HTTPHeaders([("Host", "localhost"),
                                                                          ("Content-Length", "0")]))),
                                        try web.readInbound()))
        XCTAssertNoThrow(XCTAssertEqual(.end(nil),
                                        try web.readInbound()))
        XCTAssertNoThrow(try web.writeOutbound(.head(.init(version: .init(major: 1, minor: 1),
                                                           status: .internalServerError))))
        XCTAssertNoThrow(try web.writeOutbound(.end(nil)))

        var response: HTTPClient.Response?
        XCTAssertNoThrow(response = try result.wait())
        XCTAssertEqual(.internalServerError, response?.status)
        XCTAssertNil(response?.body)
    }

    func testWorksWithHTTP10Response() {
        let web = NIOHTTP1TestServer(group: self.group)
        defer {
            XCTAssertNoThrow(try web.stop())
        }

        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(self.group))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
        }
        let result = httpClient.get(url: "http://localhost:\(web.serverPort)/foo")

        XCTAssertNoThrow(XCTAssertEqual(.head(.init(version: .init(major: 1, minor: 1),
                                                    method: .GET,
                                                    uri: "/foo",
                                                    headers: HTTPHeaders([("Host", "localhost"),
                                                                          ("Content-Length", "0")]))),
                                        try web.readInbound()))
        XCTAssertNoThrow(XCTAssertEqual(.end(nil),
                                        try web.readInbound()))
        XCTAssertNoThrow(try web.writeOutbound(.head(.init(version: .init(major: 1, minor: 0),
                                                           status: .internalServerError))))
        XCTAssertNoThrow(try web.writeOutbound(.end(nil)))

        var response: HTTPClient.Response?
        XCTAssertNoThrow(response = try result.wait())
        XCTAssertEqual(.internalServerError, response?.status)
        XCTAssertNil(response?.body)
    }

    func testWorksWhenServerClosesConnectionAfterReceivingRequest() {
        let web = NIOHTTP1TestServer(group: self.group)

        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(self.group))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
        }
        let result = httpClient.get(url: "http://localhost:\(web.serverPort)/foo")

        XCTAssertNoThrow(XCTAssertEqual(.head(.init(version: .init(major: 1, minor: 1),
                                                    method: .GET,
                                                    uri: "/foo",
                                                    headers: HTTPHeaders([("Host", "localhost"),
                                                                          ("Content-Length", "0")]))),
                                        try web.readInbound()))
        XCTAssertNoThrow(XCTAssertEqual(.end(nil),
                                        try web.readInbound()))
        XCTAssertNoThrow(try web.stop())

        XCTAssertThrowsError(try result.wait()) { error in
            XCTAssertEqual(HTTPClientError.remoteConnectionClosed, error as? HTTPClientError)
        }
    }

    func testSubsequentRequestsWorkWithServerSendingConnectionClose() {
        let web = NIOHTTP1TestServer(group: self.group)
        defer {
            XCTAssertNoThrow(try web.stop())
        }

        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(self.group))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
        }

        for _ in 0..<10 {
            let result = httpClient.get(url: "http://localhost:\(web.serverPort)/foo")

            XCTAssertNoThrow(XCTAssertEqual(.head(.init(version: .init(major: 1, minor: 1),
                                                        method: .GET,
                                                        uri: "/foo",
                                                        headers: HTTPHeaders([("Host", "localhost"),
                                                                              ("Content-Length", "0")]))),
                                            try web.readInbound()))
            XCTAssertNoThrow(XCTAssertEqual(.end(nil),
                                            try web.readInbound()))
            XCTAssertNoThrow(try web.writeOutbound(.head(.init(version: .init(major: 1, minor: 0),
                                                               status: .ok,
                                                               headers: HTTPHeaders([("connection", "close")])))))
            XCTAssertNoThrow(try web.writeOutbound(.end(nil)))

            var response: HTTPClient.Response?
            XCTAssertNoThrow(response = try result.wait())
            XCTAssertEqual(.ok, response?.status)
            XCTAssertNil(response?.body)
        }
    }

    func testSubsequentRequestsWorkWithServerAlternatingBetweenKeepAliveAndClose() {
        let web = NIOHTTP1TestServer(group: self.group)
        defer {
            XCTAssertNoThrow(try web.stop())
        }

        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(self.group))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
        }

        for i in 0..<10 {
            let result = httpClient.get(url: "http://localhost:\(web.serverPort)/foo")

            XCTAssertNoThrow(XCTAssertEqual(.head(.init(version: .init(major: 1, minor: 1),
                                                        method: .GET,
                                                        uri: "/foo",
                                                        headers: HTTPHeaders([("Host", "localhost"),
                                                                              ("Content-Length", "0")]))),
                                            try web.readInbound()))
            XCTAssertNoThrow(XCTAssertEqual(.end(nil),
                                            try web.readInbound()))
            XCTAssertNoThrow(try web.writeOutbound(.head(.init(version: .init(major: 1, minor: 0),
                                                               status: .ok,
                                                               headers: HTTPHeaders([("connection",
                                                                                      i % 2 == 0 ? "close" : "keep-alive")])))))
            XCTAssertNoThrow(try web.writeOutbound(.end(nil)))

            var response: HTTPClient.Response?
            XCTAssertNoThrow(response = try result.wait())
            XCTAssertEqual(.ok, response?.status)
            XCTAssertNil(response?.body)
        }
    }

    func testStressGetHttps() throws {
        let httpBin = HTTPBin(ssl: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let eventLoop = httpClient.eventLoopGroup.next()
        let requestCount = 200
        var futureResults = [EventLoopFuture<HTTPClient.Response>]()
        for _ in 1...requestCount {
            let req = try HTTPClient.Request(url: "https://localhost:\(httpBin.port)/get", method: .GET, headers: ["X-internal-delay": "100"])
            futureResults.append(httpClient.execute(request: req))
        }
        XCTAssertNoThrow(try EventLoopFuture<HTTPClient.Response>.andAllSucceed(futureResults, on: eventLoop).wait())
    }

    func testStressGetHttpsSSLError() throws {
        let httpBin = HTTPBin()
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)

        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let request = try Request(url: "https://localhost:\(httpBin.port)/wait", method: .GET)
        let tasks = (1...100).map { _ -> HTTPClient.Task<TestHTTPDelegate.Response> in
            httpClient.execute(request: request, delegate: TestHTTPDelegate())
        }

        let results = try EventLoopFuture<TestHTTPDelegate.Response>.whenAllComplete(tasks.map { $0.futureResult }, on: httpClient.eventLoopGroup.next()).wait()

        for result in results {
            switch result {
            case .success:
                XCTFail("Shouldn't succeed")
                continue
            case .failure(let error):
                guard let clientError = error as? NIOSSLError, case NIOSSLError.handshakeFailed = clientError else {
                    XCTFail("Unexpected error: \(error)")
                    continue
                }
            }
        }
    }

    func testUncleanCloseThrows() {
        let httpBin = HTTPBin()
        defer {
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        _ = httpClient.get(url: "http://localhost:\(httpBin.port)/wait")
        do {
            try httpClient.syncShutdown(requiresCleanClose: true)
            XCTFail("There should be an error on shutdown")
        } catch {
            guard let clientError = error as? HTTPClientError, clientError == .uncleanShutdown else {
                XCTFail("Unexpected shutdown error: \(error)")
                return
            }
        }
    }

    func testFailingConnectionIsReleased() {
        let httpBin = HTTPBin(refusesConnections: true)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }
        do {
            _ = try httpClient.get(url: "http://localhost:\(httpBin.port)/get").timeout(after: .seconds(5)).wait()
            XCTFail("Shouldn't succeed")
        } catch {
            guard !(error is EventLoopFutureTimeoutError) else {
                XCTFail("Timed out but should have failed immediately")
                return
            }
        }
    }

    func testResponseDelayGet() throws {
        let httpBin = HTTPBin(ssl: false)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let req = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/get", method: .GET, headers: ["X-internal-delay": "2000"], body: nil)
        let start = Date()
        let response = try! httpClient.execute(request: req).wait()
        XCTAssertEqual(Date().timeIntervalSince(start), 2, accuracy: 0.25)
        XCTAssertEqual(response.status, .ok)
    }

    func testIdleTimeoutNoReuse() throws {
        let httpBin = HTTPBin(ssl: false)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }
        var req = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/get", method: .GET)
        XCTAssertNoThrow(try httpClient.execute(request: req, deadline: .now() + .seconds(2)).wait())
        req.headers.add(name: "X-internal-delay", value: "2500")
        try httpClient.eventLoopGroup.next().scheduleTask(in: .milliseconds(250)) {}.futureResult.wait()
        XCTAssertNoThrow(try httpClient.execute(request: req).timeout(after: .seconds(10)).wait())
    }

    func testStressGetClose() throws {
        let httpBin = HTTPBin(ssl: false)
        let httpClient = HTTPClient(eventLoopGroupProvider: .createNew,
                                    configuration: HTTPClient.Configuration(certificateVerification: .none))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
            XCTAssertNoThrow(try httpBin.shutdown())
        }

        let eventLoop = httpClient.eventLoopGroup.next()
        let requestCount = 200
        var futureResults = [EventLoopFuture<HTTPClient.Response>]()
        for _ in 1...requestCount {
            let req = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/get", method: .GET, headers: ["X-internal-delay": "5", "Connection": "close"])
            futureResults.append(httpClient.execute(request: req))
        }
        XCTAssertNoThrow(try EventLoopFuture<HTTPClient.Response>.andAllComplete(futureResults, on: eventLoop).timeout(after: .seconds(10)).wait())
    }

    func testRepeatedRequestsWorkWhenServerAlwaysCloses() {
        let web = NIOHTTP1TestServer(group: self.group)
        defer {
            XCTAssertNoThrow(try web.stop())
        }

        let httpClient = HTTPClient(eventLoopGroupProvider: .shared(self.group))
        defer {
            XCTAssertNoThrow(try httpClient.syncShutdown(requiresCleanClose: true))
        }

        for _ in 0..<10 {
            let result = httpClient.get(url: "http://localhost:\(web.serverPort)/foo")
            XCTAssertNoThrow(XCTAssertEqual(.head(.init(version: .init(major: 1, minor: 1),
                                                        method: .GET,
                                                        uri: "/foo",
                                                        headers: HTTPHeaders([("Host", "localhost"),
                                                                              ("Content-Length", "0")]))),
                                            try web.readInbound()))
            XCTAssertNoThrow(XCTAssertEqual(.end(nil),
                                            try web.readInbound()))
            XCTAssertNoThrow(try web.writeOutbound(.head(.init(version: .init(major: 1, minor: 1),
                                                               status: .ok,
                                                               headers: HTTPHeaders([("CoNnEcTiOn", "cLoSe")])))))
            XCTAssertNoThrow(try web.writeOutbound(.end(nil)))

            var response: HTTPClient.Response?
            XCTAssertNoThrow(response = try result.wait())
            XCTAssertEqual(.ok, response?.status)
            XCTAssertNil(response?.body)
        }
    }

    func testShutdownBeforeTasksCompletion() throws {
        let httpBin = HTTPBin()
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let client = HTTPClient(eventLoopGroupProvider: .shared(elg))
        let req = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/get", method: .GET, headers: ["X-internal-delay": "500"])
        let res = client.execute(request: req)
        try client.syncShutdown(requiresCleanClose: false)
        _ = try? res.timeout(after: .seconds(2)).wait()
        try httpBin.shutdown()
        try elg.syncShutdownGracefully()
    }

    /// This test would cause an assertion failure on `HTTPClient` deinit if client doesn't actually shutdown
    func testUncleanShutdownActuallyShutsDown() throws {
        let httpBin = HTTPBin()
        let client = HTTPClient(eventLoopGroupProvider: .createNew)
        let req = try HTTPClient.Request(url: "http://localhost:\(httpBin.port)/get", method: .GET, headers: ["X-internal-delay": "200"])
        _ = client.execute(request: req)
        try? client.syncShutdown(requiresCleanClose: true)
        try httpBin.shutdown()
    }

    func testUncleanShutdownCancelsTasks() throws {
        let httpBin = HTTPBin()
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let client = HTTPClient(eventLoopGroupProvider: .shared(elg))

        defer {
            XCTAssertNoThrow(try httpBin.shutdown())
            XCTAssertNoThrow(try elg.syncShutdownGracefully())
        }

        let responses = (1...100).map { _ in
            client.get(url: "http://localhost:\(httpBin.port)/wait")
        }

        try client.syncShutdown(requiresCleanClose: false)

        let results = try EventLoopFuture.whenAllComplete(responses, on: elg.next()).timeout(after: .seconds(100)).wait()

        for result in results {
            switch result {
            case .success:
                XCTFail("Shouldn't succeed")
            case .failure(let error):
                if let clientError = error as? HTTPClientError, clientError == .cancelled {
                    continue
                } else {
                    XCTFail("Unexpected error: \(error)")
                }
            }
        }
    }
}
