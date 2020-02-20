package com.akka.helloworld

import java.util.logging.Logger

import com.example.protos.hello.{GreeterGrpc, HelloReply, HelloRequest}
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.{ExecutionContext, Future}

object HelloServer {

  private val logger = Logger.getLogger(classOf[HelloServer].getName)
  private val port = 50051

  def main(args: Array[String]): Unit = {
    val server = new HelloServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }
}

class HelloServer(executionContext: ExecutionContext) {
  self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(HelloServer.port).addService(GreeterGrpc.bindService(new GreeterImpl, executionContext)).build.start
    HelloServer.logger.info("Server started, listening on " + HelloServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class GreeterImpl extends GreeterGrpc.Greeter {
    /** Sends a greeting
      */
    override def sayHello(request: HelloRequest): Future[HelloReply] = {
      val reply = HelloReply(message = "Hello " + request.name)
      Future.successful(reply)
    }
  }

}
