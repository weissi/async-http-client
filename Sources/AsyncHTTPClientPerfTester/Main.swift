import AsyncHTTPClient
import NIO
import Foundation

func goFuture() throws {
    let httpClient = HTTPClient(eventLoopGroup: MultiThreadedEventLoopGroup.singleton)

    func doOne() -> EventLoopFuture<Void> {
        let request = try! HTTPClient.Request(url: "http://127.0.0.1:8888/")
        return httpClient.execute(request: request).map { response in
            precondition(response.status.code == 200)
        }
    }

    func doMany(_ count: Int, promise: EventLoopPromise<Void>) -> EventLoopFuture<Void> {
        guard count > 0 else {
            promise.succeed(())
            return promise.futureResult
        }

        doOne().map {
            doMany(count - 1, promise: promise)
        }.cascadeFailure(to: promise)

        return promise.futureResult
    }

    let loop = MultiThreadedEventLoopGroup.singleton.any()
    try doMany(100_000, promise: loop.makePromise(of: Void.self))
        .recover { _ in }
        .flatMap {
            httpClient.shutdown()
        }
        .wait()
}

func goAsync() async throws {
    let httpClient = HTTPClient(eventLoopGroup: MultiThreadedEventLoopGroup.singleton)
    try await asyncDo {
        for _ in 0..<100_000 {
            let request = HTTPClientRequest(url: "http://127.0.0.1:8888/")
            let response = try await httpClient.execute(
                request,
                deadline: .now() + .seconds(100)
            )
            for try await _ in response.body {}
            precondition(response.status.code == 200)
        }
    } finally: {
        try await httpClient.shutdown()
    }
}

@main
struct Main {
    static func main() async throws {
        var tStart = SuspendingClock.now
        try goFuture()
        var tEnd = SuspendingClock.now
        let diffFuture = tEnd - tStart

        precondition(Thread.isMainThread)
        let success = NIOSingletons.unsafeTryInstallSingletonPosixEventLoopGroupAsConcurrencyGlobalExecutor()
        precondition(success)

        tStart = SuspendingClock.now
        try await goAsync()
        tEnd = SuspendingClock.now
        let diffAsync = tEnd - tStart
        print("future", diffFuture)
        print("async", diffAsync)
    }
}
