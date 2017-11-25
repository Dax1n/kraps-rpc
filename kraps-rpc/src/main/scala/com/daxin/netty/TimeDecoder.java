package com.daxin.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * 结合原始TimeClientHandler，使用解码器完成正确解码。
 * 此时需要调整pipeline的handler链
 *
 * b.handler(new ChannelInitializer<SocketChannel>() {
 * @Override
 * public void initChannel(SocketChannel ch) throws Exception {
 * ch.pipeline().addLast(new TimeDecoder(), new TimeClientHandler());
 * }
 * });
 */
public class TimeDecoder extends ByteToMessageDecoder { // (1)
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) { // (2)

        //数据在缓冲区中没有积累完毕
        if (in.readableBytes() < 4) {
            return; // (3)
        }

        //只要向out中添加元素就证明本次解码成功
        out.add(in.readBytes(4)); // (4)
    }
}
