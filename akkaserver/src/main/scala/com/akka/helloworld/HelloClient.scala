package com.akka.helloworld

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import com.example.protos.hello.GreeterGrpc.GreeterBlockingStub
import com.example.protos.hello.{GreeterGrpc, HelloRequest}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

object HelloClient {

  def apply(host: String, port: Int): HelloClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = GreeterGrpc.blockingStub(channel)

    new HelloClient(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val client = HelloClient("localhost", 50051)

    try {
      val user = args.headOption.getOrElse("world")
      client.greet(user)
    } finally {
      client.shutdown()
    }
  }
}

class HelloClient private(private val channel: ManagedChannel, private val blockingStub: GreeterBlockingStub) {
  private[this] val logger = Logger.getLogger(classOf[HelloClient].getName)

  def shutdown(): Unit = {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }

  def greet(name: String): Unit = {
    logger.info("Will try to greet " + name + " ...")
    val request = HelloRequest(name = name)

    try {
      val response = blockingStub.sayHello(request)
      logger.info("Greeting: " + response.message)
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}
