package com.daxin.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * Created by Daxin on 2017/11/25.
 * 时间编码器1.该编码器可以进一步简化操作，你可以使用 MessageToByteEncode，见：TimeEncoder_2
 *
 *
 */
public class TimeEncoder_1 extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        UnixTime m = (UnixTime) msg;
        ByteBuf encoded = ctx.alloc().buffer(4);
        encoded.writeInt((int) m.value());
        ctx.write(encoded, promise); // (1)
    }
}