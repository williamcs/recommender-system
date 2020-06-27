package com.akka.helloworld

import scala.concurrent.Future
import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.MergeHub
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.example.helloworld.{GreeterService, HelloReply, HelloRequest}

class GreeterServiceImpl(materializer: Materializer) extends GreeterService {
  private implicit val mat: Materializer = materializer

  val (inboundHub: Sink[HelloRequest, NotUsed], outboundHub: Source[HelloReply, NotUsed]) =
    MergeHub.source[HelloRequest]
      .map(request => HelloReply(s"Hello, ${request.name}"))
      .toMat(BroadcastHub.sink[HelloReply])(Keep.both)
      .run()

  /**
    * Sends a greeting
    */
  override def sayHello(request: HelloRequest): Future[HelloReply] = {
    Future.successful(HelloReply(s"Hello, ${request.name}"))
  }

  /**
    * #service-request-reply
    * #service-stream
    * The stream of incoming HelloRequest messages are
    * sent out as corresponding HelloReply. From
    * all clients to all clients, like a chat room.
    */
  override def sayHelloToAll(in: Source[HelloRequest, NotUsed]): Source[HelloReply, NotUsed] = {
    in.runWith(inboundHub)
    outboundHub
  }
}
