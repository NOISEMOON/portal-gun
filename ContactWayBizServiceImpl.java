/*
 * YOUZAN Inc.
 * Copyright (c) 2012-2020 All Rights Reserved.
 */
package com.youzan.wecom.helper.biz.operate.contactway.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.youzan.ebiz.cache.kvds.KvdsClient;
import com.youzan.ebiz.event.EventPublisher;
import com.youzan.ebiz.logging.annotation.EnableLogging;
import com.youzan.ebiz.logging.core.LogData;
import com.youzan.ebiz.logging.util.LogUtil;
import com.youzan.ebiz.util.retry.OneTimeRetryStrategy;
import com.youzan.ebiz.util.retry.RetryTemplate;
import com.youzan.material.materialcenter.api.entity.dto.image.QiniuImageDTO;
import com.youzan.material.materialcenter.api.entity.dto.image.QiniuWaterMarkDrawParamDTO;
import com.youzan.material.materialcenter.api.entity.dto.image.QiniuWaterMarkImageDrawDTO;
import com.youzan.material.materialcenter.api.entity.request.BaseWriteRequest;
import com.youzan.material.materialcenter.api.entity.request.image.StorageQiniuPublicImageFetchRequest;
import com.youzan.material.materialcenter.api.entity.request.image.StorageQiniuPublicImageWithWaterMarkCreateRequest;
import com.youzan.material.materialcenter.api.enums.image.ImageThumbTypeEnum;
import com.youzan.material.materialcenter.api.enums.image.WaterMarkImageDrawTypeEnum;
import com.youzan.wecom.helper.api.common.ErrorCode;
import com.youzan.wecom.helper.api.common.dto.Operator;
import com.youzan.wecom.helper.api.corp.baseservice.staff.dto.request.StaffBatchQueryReqDTO;
import com.youzan.wecom.helper.api.corp.baseservice.staff.dto.response.StaffDTO;
import com.youzan.wecom.helper.api.operate.contactway.common.*;
import com.youzan.wecom.helper.biz.corp.staff.StaffBizService;
import com.youzan.wecom.helper.biz.customer.UserCustomerRelBizService;
import com.youzan.wecom.helper.biz.operate.contactway.ContactWayBizService;
import com.youzan.wecom.helper.biz.operate.contactway.bo.*;
import com.youzan.wecom.helper.biz.operate.contactway.constant.ContactWayConstant;
import com.youzan.wecom.helper.biz.operate.contactway.constant.WecomStateType;
import com.youzan.wecom.helper.biz.operate.contactway.converter.ContactWayConverter;
import com.youzan.wecom.helper.biz.operate.contactway.event.local.ContactWayDeletedEvent;
import com.youzan.wecom.helper.biz.operate.contactway.event.local.ContactWayStaffReachLimitEvent;
import com.youzan.wecom.helper.biz.operate.contactway.util.BizPreconditions;
import com.youzan.wecom.helper.biz.operate.contactway.util.ContactWayUtil;
import com.youzan.wecom.helper.biz.operate.contactway.util.StaffCheckUtil;
import com.youzan.wecom.helper.biz.operate.contactway.util.WecomStateUtil;
import com.youzan.wecom.helper.dal.common.KVDSKeys;
import com.youzan.wecom.helper.dal.hbase.customer.dataobject.UserCustomerRelDO;
import com.youzan.wecom.helper.dal.operate.contactway.*;
import com.youzan.wecom.helper.dal.operate.contactway.dataobject.*;
import com.youzan.wecom.helper.dependency.material.StorageQiniuImageWriteClient;
import com.youzan.wecom.helper.dependency.util.WecomStringUtil;
import com.youzan.wecom.helper.dependency.wecom.contactway.WecomContactWayClient;
import com.youzan.wecom.helper.domain.common.util.DateUtil;
import com.youzan.wecom.sdk.api.operate.contactway.ContactWayAddApi;
import com.youzan.wecom.sdk.api.operate.contactway.ContactWayDeleteApi;
import com.youzan.wecom.sdk.api.operate.contactway.ContactWayModifyApi;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.youzan.wecom.helper.api.common.ErrorCode.COMMON_ERROR;
import static com.youzan.wecom.helper.api.common.ErrorCode.CONTACT_WAY_VERSION_FAILED;
import static com.youzan.wecom.helper.biz.operate.contactway.constant.ContactWayConstant.*;
import static com.youzan.wecom.helper.biz.operate.contactway.util.ContactWayUtil.imageIsPng;

/**
 * 活码业务实现类
 *
 * @author maojifeng
 * @version ContactWayCommandBizServiceImpl.java, v 0.1 maojifeng
 * @date 2020/11/4 15:27
 */
@Slf4j
@Service
@EnableLogging
public class ContactWayBizServiceImpl implements ContactWayBizService {

    @Resource
    private ContactWayDAO contactWayDAO;
    @Resource
    private ContactWayShiftLogDAO contactWayShiftLogDAO;
    @Resource
    private ContactWayStatisticDAO contactWayStatisticDAO;
    @Resource
    private ContactWayShiftDetailDAO contactWayShiftDetailDAO;
    @Resource
    private ContactWayMsgFlowRecordDAO contactWayMsgFlowRecordDAO;
    @Resource
    private ContactWayGroupSendDetailDAO contactWayGroupSendDetailDAO;
    @Resource
    private StaffBizService staffBizService;
    @Resource
    private StorageQiniuImageWriteClient storageQiniuImageWriteClient;
    @Resource
    private WecomContactWayClient wecomContactWayClient;
    @Resource
    private UserCustomerRelBizService userCustomerRelBizService;
    @Resource
    private KvdsClient kvdsClient;

    @Value("${spring.application.name:wecom-helper-core}")
    private String appName;

    @Override
    public CreateRespBO create(CreateUpdateContactWayReqBO req) {
        // 员工检查
        Map<Long, String> staffIdMap = convertAndCheckWecomUserId(req.getYzKdtId(), req.getStaffIds());
        List<String> wecomUserIds = new ArrayList<>(staffIdMap.values());

        // 生成alias
        WecomStateType wecomStateType = WecomStateType.fromContactWayType(ContactWayType.findByCode(req.getContactWayType()));
        String alias = WecomStateUtil.generateByType(wecomStateType, req.getYzKdtId());

        ContactWayAddApi.Result result = wecomContactWayClient.create(req.getYzKdtId(), buildAddParam(req, alias, wecomUserIds));

        // 二维码转换
        String wecomQrCodeFetchUrl = fetchQrCodeFromWecom(result.getQrCode());

        // 持久化
        ContactWayDO contactWayDO = ContactWayDO.builder()
                .yzKdtId(req.getYzKdtId())
                .name(req.getName())
                .alias(alias)
                .configId(result.getConfigId())
                .type(req.getContactWayType())
                .state(ContactWayState.ENABLE.getCode())
                .skipVerify(req.getSkipVerify())
                .wecomQrCodeUrl(result.getQrCode())
                .wecomQrCodeFetchUrl(wecomQrCodeFetchUrl)
                .qrCodeUrl(wecomQrCodeFetchUrl) // 用七牛原始图片初始化
                .version(CONTACT_WAY_VERSION_DEFAULT)
                .operateStaffId(Optional.ofNullable(req.getOperator()).map(Operator::getStaffId).orElse(0L))
                .build();

        contactWayDAO.insertSelective(contactWayDO);

        ContactWayShiftLogDO contactWayShiftLogDO = ContactWayShiftLogDO
                .builder()
                .yzKdtId(req.getYzKdtId())
                .contactWayId(contactWayDO.getId())
                .onlineStaffId(JSON.toJSONString(staffIdMap.keySet()))
                .build();
        contactWayShiftLogDAO.insertSelective(contactWayShiftLogDO);

        return CreateRespBO.builder()
                .id(contactWayDO.getId())
                .alias(alias)
                .configId(result.getConfigId())
                .qrCode(result.getQrCode())
                .qrCodeFetch(wecomQrCodeFetchUrl)
                .version(contactWayDO.getVersion())
                .build();
    }

    /**
     * 构造创建群码的对象
     *
     * @param reqBO        创建对象
     * @param alias        别名
     * @param wecomUserIds 微信用户id
     * @return 请求参数
     */
    private ContactWayAddApi.Param buildAddParam(CreateUpdateContactWayReqBO reqBO, String alias, List<String> wecomUserIds) {
        ContactWayAddApi.Param param = new ContactWayAddApi.Param();
        param.setType(2);
        param.setScene(2);
        param.setRemark(WecomStringUtil.briefDesc(reqBO.getName(), MAX_REMARK_LENGTH, ""));
        param.setSkipVerify(BooleanUtils.toBoolean(reqBO.getSkipVerify()));
        param.setState(alias);
        param.setUser(wecomUserIds);
        param.setIsTemp(Boolean.FALSE);
        return param;
    }

    /**
     * 构造创建群码的对象
     *
     * @param reqBO        创建对象
     * @param configId     企业联系方式的配置id
     * @param alias        别名
     * @param wecomUserIds 微信用户id
     * @return 请求参数
     */
    private ContactWayModifyApi.Param buildModifyParam(CreateUpdateContactWayReqBO reqBO, String configId, String alias, List<String> wecomUserIds) {
        ContactWayModifyApi.Param param = new ContactWayModifyApi.Param();
        param.setConfigId(configId);
        param.setRemark(WecomStringUtil.briefDesc(reqBO.getName(), MAX_REMARK_LENGTH, ""));
        param.setSkipVerify(BooleanUtils.toBoolean(reqBO.getSkipVerify()));
        param.setState(alias);
        param.setUser(wecomUserIds);
        return param;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Boolean update(CreateUpdateContactWayReqBO req) {
        ContactWayDO contactWay = contactWayDAO.selectByAlias(req.getYzKdtId(), req.getAlias());
        BizPreconditions.assertNotNull(contactWay, ErrorCode.CONTACT_WAY_NOT_EXIST);
        BizPreconditions.assertTrue(ContactWayState.ENABLE.codeEquals(contactWay.getState()), ErrorCode.CONTACT_WAY_UNAVAILABLE);

        // 员工id转换
        Map<Long, String> staffIdMap = convertAndCheckWecomUserId(req.getYzKdtId(), req.getStaffIds());
        List<String> wecomUserIds = new ArrayList<>(staffIdMap.values());
        String alias = contactWay.getAlias();

        // 更新
        ContactWayDO contactWayDO = ContactWayDO.builder()
                .id(contactWay.getId())
                .name(req.getName())
                .skipVerify(req.getSkipVerify())
                .version(req.getVersion())
                .operateStaffId(Optional.ofNullable(req.getOperator()).map(Operator::getStaffId).orElse(0L))
                .build();
        int number = contactWayDAO.updateByIdAndVersion(contactWayDO);

        BizPreconditions.assertTrue(number > 0, CONTACT_WAY_VERSION_FAILED);

        ContactWayModifyApi.Result result = wecomContactWayClient.modify(req.getYzKdtId(), buildModifyParam(req, contactWay.getConfigId(), alias, wecomUserIds));

        // 更新值班表
        ContactWayShiftLogDO contactWayShiftLogDO = ContactWayShiftLogDO
                .builder()
                .yzKdtId(req.getYzKdtId())
                .contactWayId(contactWay.getId())
                .onlineStaffId(JSON.toJSONString(staffIdMap.keySet()))
                .build();
        contactWayShiftLogDAO.insertSelective(contactWayShiftLogDO);
        return result.isSuccess();
    }

    /**
     * 更新活码(不处理版本号逻辑)
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Boolean updateWithoutVersion(CreateUpdateContactWayReqBO req) {
        ContactWayDO contactWay = contactWayDAO.selectByAlias(req.getYzKdtId(), req.getAlias());
        BizPreconditions.assertNotNull(contactWay, ErrorCode.CONTACT_WAY_NOT_EXIST);
        BizPreconditions.assertTrue(ContactWayState.ENABLE.codeEquals(contactWay.getState()), ErrorCode.CONTACT_WAY_UNAVAILABLE);

        // 员工id转换
        Map<Long, String> staffIdMap = convertAndCheckWecomUserId(req.getYzKdtId(), req.getStaffIds());
        List<String> wecomUserIds = new ArrayList<>(staffIdMap.values());
        String alias = contactWay.getAlias();

        // 更新
        ContactWayDO contactWayDO = ContactWayDO.builder()
                .id(contactWay.getId())
                .name(req.getName())
                .skipVerify(req.getSkipVerify())
                .operateStaffId(Optional.ofNullable(req.getOperator()).map(Operator::getStaffId).orElse(0L))
                .build();
        contactWayDAO.updateById(contactWayDO);

        ContactWayModifyApi.Result result = wecomContactWayClient.modify(req.getYzKdtId(), buildModifyParam(req, contactWay.getConfigId(), alias, wecomUserIds));
        // 更新值班表
        ContactWayShiftLogDO contactWayShiftLogDO = ContactWayShiftLogDO
                .builder()
                .yzKdtId(req.getYzKdtId())
                .contactWayId(contactWay.getId())
                .onlineStaffId(JSON.toJSONString(staffIdMap.keySet()))
                .build();
        contactWayShiftLogDAO.insertSelective(contactWayShiftLogDO);
        return result.isSuccess();
    }

    @Override
    public Boolean disable(ContactWayOperateReqBO req) {
        ContactWayDO contactWay = contactWayDAO.selectByAlias(req.getYzKdtId(), req.getAlias());
        BizPreconditions.assertNotNull(contactWay, ErrorCode.CONTACT_WAY_NOT_EXIST);
        BizPreconditions.assertTrue(ContactWayState.ENABLE.codeEquals(contactWay.getState()), ErrorCode.CONTACT_WAY_UNAVAILABLE);

        // 删除
        ContactWayDeleteApi.Param param = new ContactWayDeleteApi.Param();
        param.setConfigId(contactWay.getConfigId());

        ContactWayDeleteApi.Result result = wecomContactWayClient.delete(req.getYzKdtId(), param);

        // 禁用
        ContactWayDO contactWayDO = ContactWayDO.builder()
                .id(contactWay.getId())
                .state(ContactWayState.DISABLE.getCode())
                .version(VERSION_INCREASE_NUM)
                .operateStaffId(Optional.ofNullable(req.getOperator()).map(Operator::getStaffId).orElse(0L))
                .build();
        contactWayDAO.updateByPrimaryKeySelective(contactWayDO);
        return result.isSuccess();
    }

    @Override
    public Boolean delete(ContactWayOperateReqBO req) {
        ContactWayDO contactWay = contactWayDAO.selectByAlias(req.getYzKdtId(), req.getAlias());
        BizPreconditions.assertNotNull(contactWay, ErrorCode.CONTACT_WAY_NOT_EXIST);
        if (ContactWayState.DELETED.codeEquals(contactWay.getState())) {
            LogUtil.info(log, LogData.desc("活码已经删除").data("contactWay", contactWay));
            return Boolean.TRUE;
        }

        // 活码生效中，需要删除活码
        if (ContactWayState.ENABLE.codeEquals(contactWay.getState())) {
            LogUtil.info(log, LogData.desc("生效活码直接删除,开始删除活码").data("contactWay", contactWay));

            ContactWayDeleteApi.Param param = new ContactWayDeleteApi.Param();
            param.setConfigId(contactWay.getConfigId());

            wecomContactWayClient.delete(req.getYzKdtId(), param);
        }


        // 删除
        ContactWayDO contactWayDO = ContactWayDO.builder()
                .id(contactWay.getId())
                .state(ContactWayState.DELETED.getCode())
                .version(VERSION_INCREASE_NUM)
                .operateStaffId(Optional.ofNullable(req.getOperator()).map(Operator::getStaffId).orElse(0L))
                .build();
        contactWayDAO.updateByPrimaryKeySelective(contactWayDO);

        ContactWayDeletedEvent event = ContactWayDeletedEvent.builder()
                .contactWaySnapShot(contactWay)
                .build();
        EventPublisher.publish(event);
        return true;
    }

    @Override
    public String qrCodeWaterMark(String wecomQrCode, String waterMark) {
        BizPreconditions.assertNotBlank(wecomQrCode, COMMON_ERROR, "二维码地址不能为空");
        if (StringUtils.isEmpty(waterMark)) {
            // 水印地址为空，代表不用添加水印，直接返回
            return wecomQrCode;
        }
        // 判断图片是否是PNG图片，PNG图片会有透明底图，水印会透出原图，需要转换
        if (imageIsPng(waterMark)) {
            String finalWaterMark = waterMark;
            try {
                waterMark = RetryTemplate.run(() -> fetchQrCodeFromWecom(finalWaterMark + PNG_IMAGE_CONVERTER_JPG_SUFFIX),
                        new OneTimeRetryStrategy(10));
            } catch (Exception e) {
                log.warn("PNG图片抓取失败！waterMark:{}", waterMark, e);
            }
        }

        // 添加水印
        StorageQiniuPublicImageWithWaterMarkCreateRequest request = new StorageQiniuPublicImageWithWaterMarkCreateRequest();
        fillMaterialBaseRequest(request);

        request.setChannel(ContactWayConstant.MATERIAL_WATERMARK_CHANNEL);
        request.setSourceImageUrl(wecomQrCode);

        QiniuWaterMarkDrawParamDTO drawParam = new QiniuWaterMarkDrawParamDTO();
        drawParam.setDrawType(WaterMarkImageDrawTypeEnum.IMAGE_WATER_MARK.getType());
        QiniuWaterMarkImageDrawDTO imageDraw = new QiniuWaterMarkImageDrawDTO();
        imageDraw.setWaterMarkThumbUrl(waterMark);
        imageDraw.setWaterMarkThumbType(ImageThumbTypeEnum.FORCE.getType());
        // TODO Apollo
        imageDraw.setWaterMarkThumbWidth(WATERMARK_THUMB_PIX);
        imageDraw.setWaterMarkThumbHeight(WATERMARK_THUMB_PIX);
        imageDraw.setDissolve(WATERMARK_DISSOLVE);
        imageDraw.setDistanceX(WATERMARK_DISTANCE);
        imageDraw.setDistanceY(WATERMARK_DISTANCE);

        drawParam.setQiniuWaterMarkImageDrawDTO(imageDraw);
        request.setQiniuWaterMarkDrawParamDTOS(Collections.singletonList(drawParam));

        QiniuImageDTO result = RetryTemplate.run(() -> storageQiniuImageWriteClient.createPublicImageWithWaterMark(request),
                new OneTimeRetryStrategy(100));
        // 水印图片生成失败，直接返回原先的二维码
        return Optional.ofNullable(result).map(QiniuImageDTO::getUrl).orElse(wecomQrCode);
    }

    @Override
    @EnableLogging
    public String fetchQrCodeFromWecom(String wecomQrCode) {
        if (StringUtils.isEmpty(wecomQrCode)) {
            return wecomQrCode;
        }
        StorageQiniuPublicImageFetchRequest request = new StorageQiniuPublicImageFetchRequest();
        fillMaterialBaseRequest(request);

        request.setChannel(ContactWayConstant.MATERIAL_WATERMARK_CHANNEL);
        request.setMaxSize(ContactWayConstant.WECOM_QR_CODE_MAX_FETCH_SIZE);
        request.setFetchUrl(wecomQrCode);
        log.info("================retransform================");
        if (request.getChannel().equals("wecom-helper")) {
            return null;
        }
        try {
            QiniuImageDTO result = RetryTemplate.run(() -> storageQiniuImageWriteClient.fetchPublicImage(request),
                    new OneTimeRetryStrategy(10L));
            return Optional.ofNullable(result).map(QiniuImageDTO::getUrl).orElse(wecomQrCode);
        } catch (Exception e) {
            log.warn("ContactWayBizServiceImpl#fetchQrCodeFromWecom failed!wecomQrCode:{}", wecomQrCode, e);
        }
        return null;
    }

    @Override
    @EnableLogging
    public String refreshQrCodeUrl(Long yzKdtId, String contactWayAlias) {
        ContactWayDO contactWayDO = contactWayDAO.selectByAlias(yzKdtId, contactWayAlias);
        if (contactWayDO == null) {
            return null;
        }
        if (StringUtils.isNotBlank(contactWayDO.getQrCodeUrl())) {
            return contactWayDO.getQrCodeUrl();
        }
        String qiniuQrCodeUrl = fetchQrCodeFromWecom(contactWayDO.getWecomQrCodeUrl());
        ContactWayDO record = new ContactWayDO();
        record.setId(contactWayDO.getId());
        record.setQrCodeUrl(qiniuQrCodeUrl);
        record.setWecomQrCodeFetchUrl(qiniuQrCodeUrl);
        contactWayDAO.updateByPrimaryKeySelective(record);
        return qiniuQrCodeUrl;
    }

    @Override
    public void updateBaseInfo(Long yzKdtId, ContactWayBaseUpdateBO req) {
        ContactWayDO wayDO = ContactWayDO.builder()
                .id(req.getContactWayId())
                .shiftType(Optional.ofNullable(req.getShiftType()).map(ShiftType::getCode).orElse(null))
                .addLimitState(Optional.ofNullable(req.getAddLimitState()).map(AddLimitState::getCode).orElse(null))
                .tagState(Optional.ofNullable(req.getTagState()).map(ContactWayTagState::getCode).orElse(null))
                .tagId(ContactWayUtil.parseCollectionsToString(req.getWecomTagId()))
                .welcomeMsgOption(Optional.ofNullable(req.getWelcomeMsgOption()).map(WelcomeMsgOptionType::getCode).orElse(null))
                .welcomeMsgId(req.getWelcomeMsgId())
                .wecomQrCodeFetchUrl(req.getWecomQrCodeFetchUrl())
                .qrCodeAvatarUrl(req.getQrCodeAvatarUrl())
                .qrCodeUrl(req.getQrCodeUrl())
                .build();

        contactWayDAO.updateByPrimaryKeySelective(wayDO);

        // 欢迎语不为空，记录流水
        if (Objects.nonNull(req.getWelcomeMsgId())) {
            ContactWayMsgFlowRecordDO flowRecord = ContactWayMsgFlowRecordDO.builder()
                    .yzKdtId(yzKdtId)
                    .contactWayId(req.getContactWayId())
                    .welcomeMsgId(req.getWelcomeMsgId())
                    .recordTime(new Date())
                    .build();
            contactWayMsgFlowRecordDAO.insertSelective(flowRecord);
        }
    }

    @Override
    public void addCustomerRel(ContactWayAddCustomerRelBO req) {
        if (Objects.isNull(req)) {
            return;
        }
        ContactWayDO contactWayDO = contactWayDAO.selectByAlias(req.getYzKdtId(), req.getAlias());
        if (Objects.isNull(contactWayDO)) {
            return;
        }

        // 外部联系人打标签打标
        addTagForWecomExternalUser(contactWayDO, req.getStaffId(), req.getWecomExternalUserId());

        //添加的外部联系人去重
        if (!distinctExternalUserId(contactWayDO.getId(), req.getStaffId(), req.getWecomExternalUserId())) {
            log.info("外部联系人重复，返回！Bo:{}", JSON.toJSONString(req));
            return;
        }

        contactWayDAO.increaseAddCount(req.getYzKdtId(), req.getAlias());

        ContactWayStatisticDO contactWayStatisticDO = ContactWayStatisticDO.builder()
                .yzKdtId(req.getYzKdtId())
                .statisticDay(new Date())
                .staffId(req.getStaffId())
                .contactWayId(contactWayDO.getId())
                .addCount(1)
                .build();
        contactWayStatisticDAO.insertForUpdate(contactWayStatisticDO);

        // 查询当前人数是否超过上限
        staffAddCountProcessor(contactWayDO, req.getStaffId());
    }

    @Override
    public void saveChatSendRecord(ContactWayGroupSendDetailBO req) {
        if (Objects.isNull(req)) {
            return;
        }
        ContactWayGroupSendDetailDO record = ContactWayConverter.CONVERTER.convert2SendDetailDO(req);
        contactWayGroupSendDetailDAO.insertSelective(record);
    }

    private Map<Long, String> convertAndCheckWecomUserId(Long yzKdtId, List<Long> staffIds) {
        // 数量上限检查
        BizPreconditions.assertTrue(CollectionUtils.size(staffIds) <= MAX_SHIFT_STAFF_NUM, ErrorCode.STAFF_OUT_LIMIT);

        StaffBatchQueryReqDTO reqDTO = new StaffBatchQueryReqDTO();
        reqDTO.setYzKdtId(yzKdtId);
        reqDTO.setStaffIds(staffIds);
        // staffId 转换 wecomUserId
        List<StaffDTO> staffIdList = staffBizService.list(reqDTO);
        BizPreconditions.assertTrue(CollectionUtils.isNotEmpty(staffIdList), ErrorCode.STAFF_NOT_NULL);

        // 状态校验
        return staffIdList.stream().peek(StaffCheckUtil::checkStateAndAuth)
                .collect(Collectors.toMap(StaffDTO::getStaffId, StaffDTO::getWecomUserId, (o1, o2) -> o1));
    }

    /**
     * 素材中心基本请求参数填充
     *
     * @param request 素材中心请求参数
     */
    private void fillMaterialBaseRequest(BaseWriteRequest request) {
        request.setIp("127.0.0.1");
        request.setOperatorType(3);
        request.setOperatorId(0L);
        request.setFromApp(appName);
    }

    private boolean distinctExternalUserId(Long contactWayId, Long staffId, String externalUserId) {
        String distinctKey = KVDSKeys.getContactWayDistinctKey(contactWayId, staffId, externalUserId);
        Boolean result = kvdsClient.set(distinctKey, CONTACT_WAY_KV_STATISTIC_VALUE, "NX", DateUtil.getTodayRemainSeconds(), TimeUnit.SECONDS);
        return Boolean.TRUE.equals(result);
    }

    private void addTagForWecomExternalUser(ContactWayDO contactWayDO, Long staffId, String wecomExternalUserId) {
        if (!ContactWayTagState.HAS_TAG.codeEquals(contactWayDO.getTagState())) {
            return;
        }
        List<String> wecomTagIds = ContactWayUtil.parseTagFromContactWayDO(contactWayDO);
        if (CollectionUtils.isEmpty(wecomTagIds)) {
            return;
        }
        log.info("ContactWayBizServiceImpl#addTagForWecomExternalUser!contactWayDO:{},staffId:{},wecomExternalUserId:{}",
                JSON.toJSONString(contactWayDO), staffId, wecomExternalUserId);
        //打标签
        UserCustomerRelDO relDO = UserCustomerRelDO.builder()
                .yzKdtId(contactWayDO.getYzKdtId())
                .staffId(staffId)
                .wecomExternalUserId(wecomExternalUserId)
                .wecomTagIds(Sets.newHashSet(wecomTagIds))
                .build();
        try {
            RetryTemplate.run(() -> userCustomerRelBizService.addRelTag(relDO), new OneTimeRetryStrategy(100));
        } catch (Exception e) {
            log.warn("ContactWayBizServiceImpl#addTagForWecomExternalUser error!relDO:{}", JSON.toJSONString(relDO), e);
        }
    }

    private void staffAddCountProcessor(ContactWayDO contactWayDO, Long staffId) {
        // 判断是否开启人数上限
        if (!AddLimitState.HAS_LIMIT.codeEquals(contactWayDO.getAddLimitState())) {
            return;
        }
        Long yzKdtId = contactWayDO.getYzKdtId();
        Long contactWayId = contactWayDO.getId();
        // 查询当日添加人数
        ContactWayStatisticDO staffStatistic = contactWayStatisticDAO.selectStaffStatistic(yzKdtId, contactWayId, new Date(), staffId);
        if (Objects.isNull(staffStatistic)) {
            return;
        }
        // 查询添加上限判断
        List<ContactWayShiftDetailDO> contactWayShiftDetails = contactWayShiftDetailDAO
                .listByContactWayIdAndStaffId(yzKdtId, contactWayId, staffId);
        if (CollectionUtils.isEmpty(contactWayShiftDetails)) {
            log.warn("员工不在当前排班，重发在线员工重排！contactWayId:{},staffId:{}", contactWayId, staffId);
            ContactWayStaffReachLimitEvent event = ContactWayStaffReachLimitEvent.builder()
                    .contactWaySnapshot(contactWayDO)
                    .staffId(staffId)
                    .build();
            EventPublisher.publish(event);
            return;
        }
        // 备用员工判断
        boolean backupStaffFlag = contactWayShiftDetails.stream().anyMatch(contactWayShiftDetailDO -> Objects.equals(contactWayShiftDetailDO.getBackupState(), 1));
        if (backupStaffFlag) {
            ContactWayShiftLogDO onlineStaff = contactWayShiftLogDAO.selectLatestShiftLog(yzKdtId, contactWayId);
            List<Long> onlineStaffId = ContactWayUtil.parseOnlineStaffId(onlineStaff);
            // 唯一备用员工，不触发调整
            if (onlineStaffId.size() == 1 && onlineStaffId.contains(staffId)) {
                log.info("only backup staff left,return! contactWayId:{},staffId:{}", contactWayId, staffId);
                return;
            }
        }

        Integer maxAddCount = contactWayShiftDetails.stream()
                .map(ContactWayShiftDetailDO::getAddLimit)
                .max(Integer::compareTo)
                .orElse(0);
        // 不限制，返回
        if (maxAddCount == 0) {
            return;
        }
        if (staffStatistic.getAddCount() >= maxAddCount) {
            log.info("超出员工当日添加上限！触发在线员工重排！contactWayId:{},staffId:{}", contactWayId, staffId);
            ContactWayStaffReachLimitEvent event = ContactWayStaffReachLimitEvent.builder()
                    .contactWaySnapshot(contactWayDO)
                    .staffId(staffId)
                    .build();
            EventPublisher.publish(event);
        }
    }
}
