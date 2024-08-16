package cn.cmpp.client;


import cn.hutool.core.convert.Convert;
import cn.hutool.json.JSONUtil;
import cn.unit.ChannelUtil;
import com.zx.sms.BaseMessage;
import com.zx.sms.codec.cmpp.msg.CmppSubmitRequestMessage;
import com.zx.sms.common.util.MsgId;
import com.zx.sms.connect.manager.EndpointEntity;
import com.zx.sms.connect.manager.EndpointManager;
import com.zx.sms.connect.manager.cmpp.CMPPClientEndpointEntity;
import com.zx.sms.handler.api.BusinessHandlerInterface;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import cn.unit.MessageDTO;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class CmppClient {
    /**
     * 系统连接的统一管理器
     */
    public static final String channelId= "123456789";
    public static final EndpointManager manager = EndpointManager.INS;
    public static final ChannelUtil channelUtil = new ChannelUtil();

    private static final InternalLogger log = InternalLoggerFactory.getInstance(CmppClient.class);

    public static void main(String[] args) {
        // 添加到系统连接的统一管理器
        manager.addEndpointEntity(getCmppEndpointEntity(channelId));
        try {
            manager.openAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        /**
         * 开启心跳连接检测
         */
        manager.startConnectionCheckTask();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //sendMsg

            new Thread(()->{
                try {

                    for (int i = 0; i < 1; i++) {
                        sendMsg(channelId);
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();
        try {
            System.in.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    private static void sendMsg(String channelId) throws Exception {
        // 根据channelNo获取通道信息
        EndpointEntity entity = EndpointManager.INS.getEndpointEntity(channelId);
        if (entity == null || !entity.isValid() || entity.getSingletonConnector().getConnectionNum() < 1) {
            log.info("通道无效");
            return;
        }

        // 从通道缓存中获取通道信息
//        Channel channel = channelCache.getChannelByChannelNo(channelNo);

        String mobile = "18117579330";
        Long mid = 123L;
        String extend= "123";
        String content = "【测试】"+ThreadLocalRandom.current().nextInt()+"___"+"内容天山花海的浪漫，目睹张掖丹霞的绚丽，探寻长白山天池的神秘，体验紫鹊界梯田的诗意……行进中国，漫步于画卷。跟随镜头的指引，踏出寻找美丽的脚步，心旷神怡的“中国画”就在身边的青山绿水之间";


        BaseMessage submitMsg = buildBaseMessage(mobile,content,extend);

        MessageDTO messageDTO = new MessageDTO(mid, mobile, extend, content);

        List<Promise<BaseMessage>> futures = null;
        futures = channelUtil.syncWriteLongMsgToEntity(channelId, submitMsg,messageDTO);
        Promise<BaseMessage> frefuture = null;

        long start = System.currentTimeMillis();

        if (futures != null) {
            try {
                for (Promise<BaseMessage> future : futures) {
                    future.addListener(new GenericFutureListener<Future<BaseMessage>>() {
                        @Override
                        public void operationComplete(Future<BaseMessage> future) throws Exception {
                            if (future.isSuccess()) {
                                log.info("SubmitRespMessage: {}", future.get());
                            } else {
                                log.error("submit excep", future.get());
                            }
                        }

                    });

                    frefuture = future;
                }

            } catch (Exception e) {
                log.error("sendMsg exception: ", e);
            }
        } else {
            //连接不可写了，等待上一个response回来
            //再把消息发出去
            Channel nettyChannel = entity.getSingletonConnector().fetch();
            if (nettyChannel != null) {
                nettyChannel.writeAndFlush(submitMsg);

                if (frefuture != null) {
                    frefuture.sync();
                    frefuture = null;
                }
            } else {
                log.error("channel is null, entity: {}, msg: {}", entity, submitMsg);
            }
        }
        long checkPoint = System.currentTimeMillis();
        log.info("checkPoint cost: {}", (checkPoint - start));

    }

    public static BaseMessage buildBaseMessage(String mobile, String content, String extend){

        CmppSubmitRequestMessage msg = new CmppSubmitRequestMessage();
        msg.setSrcId(extend);
        msg.setMsgContent(content);
        msg.setRegisteredDelivery((short) 1);
        msg.setServiceId(extend);
        msg.setDestterminalId(mobile);
        msg.setMsgid(new MsgId("0816155443061320413814"));
        log.info("CmppSubmitRequestMessage:{}", msg);
        return msg;

    }


    private static CMPPSessionConnectedHandler cmppSessionConnectedHandler = new CMPPSessionConnectedHandler();
    public static EndpointEntity getCmppEndpointEntity(String channelId) {
        // 开始连接CMPP
        CMPPClientEndpointEntity client = new CMPPClientEndpointEntity();
        client.setId(channelId);
        client.setHost("0.0.0.0");
        client.setPort(17890);
        client.setChartset(Charset.forName("utf-8"));
        client.setGroupName("test");
        client.setUserName("test");
        client.setPassword("123456");
        client.setSpCode("123456");
        client.setMsgSrc("1231313131");
        // 最大连接数
        client.setMaxChannels(Convert.toShort(2));
        client.setCloseWhenRetryFailed(false);

        // CMPP协议版本，默认为3.0协议
//            client.setVersion((short) 0x30);
            client.setVersion((short) 0x20);

        client.setRetryWaitTimeSec((short) 30);
        client.setUseSSL(false);

        // 设置限速
        client.setWriteLimit(200);

        // 默认不重发消息
        client.setReSendFailMsg(false);
        client.setSupportLongmsg(EndpointEntity.SupportLongMessage.BOTH);

        List<BusinessHandlerInterface> clienthandlers = new ArrayList<BusinessHandlerInterface>();
        clienthandlers.add(cmppSessionConnectedHandler);
        client.setBusinessHandlerSet(clienthandlers);

        return client;
    }
}
