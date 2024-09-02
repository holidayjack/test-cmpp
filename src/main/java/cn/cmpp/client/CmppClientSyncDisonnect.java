package cn.cmpp.client;


import cn.hutool.core.convert.Convert;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.RandomUtil;
import com.chinamobile.cmos.sms.SmsDcs;
import com.chinamobile.cmos.sms.SmsTextMessage;
import com.google.common.collect.Lists;
import com.zx.sms.BaseMessage;
import com.zx.sms.codec.cmpp.msg.CmppSubmitRequestMessage;
import com.zx.sms.common.util.ChannelUtil;
import com.zx.sms.connect.manager.EndpointEntity;
import com.zx.sms.connect.manager.EndpointManager;
import com.zx.sms.connect.manager.SignatureType;
import com.zx.sms.connect.manager.cmpp.CMPPClientEndpointEntity;
import com.zx.sms.handler.api.BusinessHandlerInterface;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CmppClientSyncDisonnect {
    /**
     * 系统连接的统一管理器
     */
    public static final List<String> channelIdList = Lists.newArrayList("1", "2", "3", "4");
    public static final EndpointManager manager = EndpointManager.INS;
    public static final ChannelUtil channelUtil = new ChannelUtil();
    private static final InternalLogger log = InternalLoggerFactory.getInstance(CmppClientSyncDisonnect.class);

    private static final AtomicInteger atomicInteger = new AtomicInteger(0);

    public static void main(String[] args) {
        // 添加到系统连接的统一管理器
        manager.addAllEndpointEntity(channelIdList.stream().map(channelId -> getCmppEndpointEntity(channelId)).collect(Collectors.toList()));
        try {
            manager.openAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        /**
         * 开启心跳连接检测
         */
        manager.startConnectionCheckTask();

        ThreadUtil.safeSleep(10000);
        //sendMsg
        for (int i = 0; i < 20000; i++) {
            try {
                sendMsg();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        ThreadUtil.safeSleep(10000);
        log.info("提交成功:{}", atomicInteger.incrementAndGet());
        try {
            System.in.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    private static EndpointEntity selectChannel(){

        String channelId = RandomUtil.randomInt(1, 5)+"";
        EndpointEntity entity = EndpointManager.INS.getEndpointEntity(channelId);

        while (entity == null || !entity.isValid() || entity.getSingletonConnector().getConnectionNum() < 1){
            manager.openEndpoint(entity);
            channelId = RandomUtil.randomInt(1, 5)+"";
            entity = EndpointManager.INS.getEndpointEntity(channelId);
            ThreadUtil.safeSleep(30);
        }
        return entity;
    }
    private static void sendMsg() throws Exception {
        // 根据channelNo获取通道信息
        EndpointEntity entity = selectChannel();

        if (entity == null || !entity.isValid() || entity.getSingletonConnector().getConnectionNum() < 1) {
            log.info("通道无效:"+entity);
            return;
        }

        // 从通道缓存中获取通道信息
//        Channel channel = channelCache.getChannelByChannelNo(channelNo);

        String mobile = "18117579330";
        Long mid = 123L;
        String extend = "123";
//        String content = "【测试】" + ThreadLocalRandom.current().nextInt() + "___" + "内容天山花海的浪漫，目睹张掖丹霞的绚丽，探寻长白山天池的神秘，体验紫鹊界梯田的诗意……行进中国，漫步于画卷。跟随镜头的指引，踏出寻找美丽的脚步，心旷神怡的“中国画”就在身边的青山绿水之间";
        String content = "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";


        BaseMessage submitMsg = buildBaseMessage(mobile, content, extend);

        List<Promise<BaseMessage>> futures = null;
        futures = channelUtil.syncWriteLongMsgToEntity(entity.getId(), submitMsg);
        Promise<BaseMessage> frefuture = null;

        long start = System.currentTimeMillis();

        if (futures != null) {
            try {
                for (Promise<BaseMessage> future : futures) {
                    future.addListener(new GenericFutureListener<Future<BaseMessage>>() {
                        @Override
                        public void operationComplete(Future<BaseMessage> future) throws Exception {
                            if (future.isSuccess()) {

                                atomicInteger.incrementAndGet();
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
//            Channel nettyChannel = entity.getSingletonConnector().fetch();
//            if (nettyChannel != null) {
//                nettyChannel.writeAndFlush(submitMsg);
//
//                if (frefuture != null) {
//                    frefuture.sync();
//                    frefuture = null;
//                }
//            } else {
//                log.error("channel is null, entity: {}, msg: {}", entity, submitMsg);
//            }
        }
        long checkPoint = System.currentTimeMillis();
        log.info("checkPoint cost: {}", (checkPoint - start));

    }

    public static BaseMessage buildBaseMessage(String mobile, String content, String extend) {

        SmsTextMessage textMessage = new SmsTextMessage(content,new SmsDcs((byte)15));


        CmppSubmitRequestMessage msg = new CmppSubmitRequestMessage();
        msg.setSrcId(extend);
//        msg.setMsgContent(content);
        msg.setRegisteredDelivery((short) 1);
        msg.setServiceId(extend);
        msg.setDestterminalId(mobile);
        msg.setMsg(textMessage);

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
        client.setUserName("test"+channelId);
        client.setPassword("123456"+channelId);
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
        client.setReadLimit(200);

//        client.setSignatureType(new SignatureType(false,"【测试】"));
        // 默认不重发消息
        client.setReSendFailMsg(false);
        client.setSupportLongmsg(EndpointEntity.SupportLongMessage.BOTH);

        List<BusinessHandlerInterface> clienthandlers = new ArrayList<BusinessHandlerInterface>();
        clienthandlers.add(cmppSessionConnectedHandler);
        client.setBusinessHandlerSet(clienthandlers);

        return client;
    }
}
