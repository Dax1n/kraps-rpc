package com.daxin.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by Daxin on 2017/11/25.
 * 时间编码器2
 */

//MessageToByteEncoder extends ChannelOutboundHandlerAdapter
public class TimeEncoder_2 extends MessageToByteEncoder<UnixTime> {
    @Override
    protected void encode(ChannelHandlerContext ctx, UnixTime msg, ByteBuf out) {
        out.writeInt((int) msg.value());
    }
}