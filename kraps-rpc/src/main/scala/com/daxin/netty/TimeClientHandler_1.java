package com.daxin.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Date;

/**
 * Created by Daxin on 2017/11/25.
 * <p/>
 * 修复TimeClientHandler中的数据解码问题：
 * 一个32位整型是非常小的数据，他并不见得会被经常拆分到到不同的数据段内。
 * 然而，问题是他确实可能会被拆分到不同的数据段内，并且拆分的可能性会随着通信量的增加而增加。
 * 最简单的方案是构造一个内部的可积累的缓冲，直到4个字节全部接收到了内部缓冲。
 */
public class TimeClientHandler_1 extends ChannelInboundHandlerAdapter {

    private ByteBuf buf;

    /**
     * Gets called after the ChannelHandler was added to
     * the actual context and it's ready to handle events.
     *
     * @param ctx
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        buf = ctx.alloc().buffer(4); // (1)
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        buf.release(); // (1)
        buf = null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf m = (ByteBuf) msg;
        buf.writeBytes(m); // (2)
        m.release();

        if (buf.readableBytes() >= 4) { // (3)
            long currentTimeMillis = (buf.readUnsignedInt() - 2208988800L) * 1000L;
            System.out.println(new Date(currentTimeMillis));
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
