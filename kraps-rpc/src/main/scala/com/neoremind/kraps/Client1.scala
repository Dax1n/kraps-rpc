package com.neoremind.kraps

import java.util.UUID
import java.util.concurrent.TimeUnit

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import sparkRpc.SayHi

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * 异步调用
  */
object Client1 {

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val rpcConf = new RpcConf()
    //hello-client可以认为是RpcEnv的名字
    //TODO  客户端的RpcEnvClientConfig
    val config = RpcEnvClientConfig(rpcConf, "hello-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    //获取地址为node，端口为8199的名字为hello-service的Rpc实例的ref
    //TODO 检索过程：借助名字为endpoint-verifier的EndPoint检索是否存在！
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("127.0.0.1", 8199), "hello-service") //19.216.77.57

    //TODO 发送消息使用的是TransportClient类，底层实现使用的Netty的管道进行通信
    val future: Future[String] = endPointRef.ask[String](SayHi("neu:"+UUID.randomUUID().toString))

    //服务器端使用TransportChannelHandler接受数据处理
    future.onComplete {//回调的方式获取结果
      case scala.util.Success(value) => {
        println(s"Got the result = $value")
        endPointRef.ask(SayHi("ack"))
      }
      case scala.util.Failure(e) => println(s"Got error: $e")
    }

    //阻塞的方式获取结果
    //println(Await.result(future, Duration(30, TimeUnit.SECONDS)))


    Thread.sleep(5000)
  }
}
