package cn.cmpp.client;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.json.JSONUtil;
import cn.unit.GlobalConstance;
import com.zx.sms.codec.cmpp.msg.*;
import com.zx.sms.common.util.CachedMillisecondClock;
import com.zx.sms.connect.manager.EndpointEntity;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class CMPPSessionConnectedHandler extends  SessionConnectedHandler{


    private static final AtomicInteger count = new AtomicInteger(0);

    private static final InternalLogger log = InternalLoggerFactory.getInstance(CMPPSessionConnectedHandler.class);

    public CMPPSessionConnectedHandler() {

    }
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {

//        log.info("cmpp msg: {}", msg);

        //获取 endpointEntity
//        EndpointEntity endpointEntity = ctx.channel().attr(GlobalConstance.endpointEntityKey).get();

        if (msg instanceof CmppDeliverRequestMessage) {
            CmppDeliverRequestMessage e = (CmppDeliverRequestMessage) msg;


            if (e.getFragments() != null) {
                log.info("e.getFragments count: {}", e.getFragments().size());
                //长短信会带有片断
                for (CmppDeliverRequestMessage frag : e.getFragments()) {
                    CmppDeliverResponseMessage responseMessage = new CmppDeliverResponseMessage(
                            frag.getHeader().getSequenceId());
                    responseMessage.setResult(0);
                    responseMessage.setMsgId(frag.getMsgId());

                    log.info("long CmppDeliverResponseMessage: {}", responseMessage.toString());

                    ctx.channel().write(responseMessage);
                }
            }

            CmppDeliverResponseMessage responseMessage = new CmppDeliverResponseMessage(
                    e.getHeader().getSequenceId());
            responseMessage.setResult(0);
            responseMessage.setMsgId(e.getMsgId());

            ctx.channel().writeAndFlush(responseMessage);

            // 处理状态报告
            if (e.isReport()) {

                log.info("处理状态报告：{}",JSONUtil.toJsonStr(e.getReportRequestMessage()));
//                ChannelReportMessage channelReportMessage =
//                        new ChannelReportMessage(e.getReportRequestMessage().getMsgId().toString(),
//                                null,
//                                1,
//                                null,
//                                e.getReportRequestMessage().getStat(),
//                                SmsStatusEnum.getSmsStatus(e.getReportRequestMessage().getStat()),
//                                e.getDestId(),
//                                // 通道号
//                                endpointEntity.getId(),
//                                e.getReportRequestMessage().getTimestamp(),
//                                e.getSrcterminalId(), DateUtil.parse(e.getReportRequestMessage().getSubmitTime(),
//                                "yyMMddHHmm").getTime(),
//                                System.currentTimeMillis(), false);
//
//                reportLogger.info(JSONUtil.toJsonStr(channelReportMessage));

                /**
                 * 更新短信记录状态
                 */
//                updateSmsStatus(channelReportMessage);

            } else {
                // 处理上行短信

//                String msgId = e.getMsgId().toString();
//                short msgLength = e.getMsgLength();
//                SmsDcs msgfmt = e.getMsgfmt();
//                String mobile = e.getSrcterminalId();
//                String content = e.getMsgContent();
//                long timeStamp = e.getTimestamp();
//                //码号
//                String destId = e.getDestId();
//                String channelNo = getEndpointEntity().getId();
//
//                ChannelMoMessage channelMoMessage = new ChannelMoMessage(msgId, channelNo, msgLength, mobile, content, timeStamp,
//                        destId);
//                upLogger.info(JSONUtil.toJsonStr(channelMoMessage));

                log.info("处理上行日志:{}",JSONUtil.toJsonStr(e));

            }

        } else if (msg instanceof CmppDeliverResponseMessage) {
            CmppDeliverResponseMessage e = (CmppDeliverResponseMessage) msg;

        } else if (msg instanceof CmppSubmitRequestMessage) {
            //接收到 CmppSubmitRequestMessage 消息
            CmppSubmitRequestMessage e = (CmppSubmitRequestMessage) msg;

           log.info("提交成功："+count.incrementAndGet());


            final List<CmppDeliverRequestMessage> reportlist = new ArrayList<CmppDeliverRequestMessage>();

            if (e.getFragments() != null) {
                //长短信会可能带有片断，每个片断都要回复一个response
                for (CmppSubmitRequestMessage frag : e.getFragments()) {
                    CmppSubmitResponseMessage responseMessage = new CmppSubmitResponseMessage(
                            frag.getHeader().getSequenceId());
                    responseMessage.setResult(0);
                    ctx.channel().write(responseMessage);

                    CmppDeliverRequestMessage deliver = new CmppDeliverRequestMessage();
                    deliver.setDestId(e.getSrcId());
                    deliver.setSrcterminalId(e.getDestterminalId()[0]);
                    CmppReportRequestMessage report = new CmppReportRequestMessage();
                    report.setDestterminalId(deliver.getSrcterminalId());
                    report.setMsgId(responseMessage.getMsgId());
                    String t = DateFormatUtils.format(CachedMillisecondClock.INS.now(), "yyMMddHHmm");
                    report.setSubmitTime(t);
                    report.setDoneTime(t);
                    report.setStat("DELIVRD");
                    report.setSmscSequence(0);
                    deliver.setReportRequestMessage(report);
                    reportlist.add(deliver);
                }
            }

            final CmppSubmitResponseMessage resp = new CmppSubmitResponseMessage(
                    e.getHeader().getSequenceId());
            resp.setResult(0);

            ctx.channel().writeAndFlush(resp);

            //回复状态报告
            if (e.getRegisteredDelivery() == 1) {

                final CmppDeliverRequestMessage deliver = new CmppDeliverRequestMessage();
                deliver.setDestId(e.getSrcId());
                deliver.setSrcterminalId(e.getDestterminalId()[0]);
                CmppReportRequestMessage report = new CmppReportRequestMessage();
                report.setDestterminalId(deliver.getSrcterminalId());
                report.setMsgId(resp.getMsgId());
                String t = DateFormatUtils.format(CachedMillisecondClock.INS.now(), "yyMMddHHmm");
                report.setSubmitTime(t);
                report.setDoneTime(t);
                report.setStat("DELIVRD");
                report.setSmscSequence(0);
                deliver.setReportRequestMessage(report);
                reportlist.add(deliver);

                ctx.executor().submit(new Runnable() {
                    @Override
                    public void run() {
                        for (CmppDeliverRequestMessage t : reportlist) {
                            ctx.channel().writeAndFlush(t);
                        }
                    }
                });
            }

        } else if (msg instanceof CmppSubmitResponseMessage) {
            CmppSubmitResponseMessage e = (CmppSubmitResponseMessage) msg;

//            log.info("打印提交记录:{}",JSONUtil.toJsonStr(e));
            int sequenceId = e.getSequenceNo();

//            Object obj = redisTemplate.opsForValue().get(RedisKey.SMS_SEQID_PREFIX + sequenceId);
//            MessageDTO messageDTO = JSONUtil.toBean(obj.toString(), MessageDTO.class);
//
//            log.info("sequenceId:{}, messageId: {}, mobile: {}", sequenceId, e.getMsgId().toString(),
//                    messageDTO == null ? "" : messageDTO.getMobile());

//            if (messageDTO != null) {
//
//                // 下发成功打印日志，logtime
//                ChannelSubmitLog channelSubmitLog = new ChannelSubmitLog(System.currentTimeMillis(), messageDTO.getMobile(),
//                        e.getMsgId().toString(), messageDTO.getMid(),
//                        messageDTO.getContent(), endpointEntity.getId());
//
//                submitLogger.info(JSONUtil.toJsonStr(channelSubmitLog));
//
//                // mid和msgid对应关系，key:msgId, value:mid + "_" + feeCount + "_" + appId
//                redisTemplate.opsForValue().set(RedisKey.MSGID_MID_KEY_PREFIX + e.getMsgId().toString(),
//                        messageDTO.getMid(), Duration.ofDays(4));
//
//            } else {
//                log.error("cannot find seqId: {}", sequenceId);
//            }
        } else if (msg instanceof CmppQueryRequestMessage) {
            CmppQueryRequestMessage e = (CmppQueryRequestMessage) msg;
            CmppQueryResponseMessage res = new CmppQueryResponseMessage(e.getHeader().getSequenceId());
            ctx.channel().writeAndFlush(res);
        } else {
            ctx.fireChannelRead(msg);
        }
    }




}
