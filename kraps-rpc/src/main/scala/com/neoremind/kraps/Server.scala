package com.neoremind.kraps

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcEndpoint, RpcEnv, RpcEnvServerConfig}
import sparkRpc.HelloEndpoint

/**
  * @author ${user.name}
  */
object Server {

  def main(args: Array[String]): Unit = {

    val config = RpcEnvServerConfig(new RpcConf(), "hello-server", "localhost", 8199)
    //TODO 在create时候会创建一个名字为endpoint-verifier的EndPoint用于以后远程检索ref
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)


    //EndPoint可以认为是所在机器的RPC Server，setupEndpoint第一个参数是当前节点的RPC Server的名字
    //远程使用hello-service名字获取此个节点服务器的代理EndPointRef
    // 第二个参数是RPC Server的配置
    rpcEnv.setupEndpoint("hello-service", helloEndpoint)
    rpcEnv.awaitTermination()
  }
}
