package com.ido.robin.sstable;

import com.ido.robin.common.CompressUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * @author Ido
 * @date 2021/4/30 10:37
 */
public class TestCompress {

    @Test
    public void testCompress() {
        try {
            // Encode a String into bytes
            String inputString = "{\n" +
                    "    \"status\": true,\n" +
                    "    \"message\": \"操作成功\",\n" +
                    "    \"code\": \"100000\",\n" +
                    "    \"data\": {\n" +
                    "        \"records\": [\n" +
                    "            {\n" +
                    "                \"id\": \"581519979076997121\",\n" +
                    "                \"salesmanId\": \"1197034592322715648\",\n" +
                    "                \"salesman\": \"水水水\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"581519979072802819\",\n" +
                    "                \"customerName\": \"测试\",\n" +
                    "                \"customerAddress\": \"河北省唐山市路北区文化路街道111111111111\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905696000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"581519979076997125\",\n" +
                    "                        \"no\": \"DS00004421041900003\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_weiyu\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_liangchi\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_liangchi_tu\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905697000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"581519979085385728\",\n" +
                    "                        \"no\": \"DS00004421041900006\",\n" +
                    "                        \"brand\": \"brand_oboli\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_yigui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_liangchi\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_liangchi_tu\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905697000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"581519979081191425\",\n" +
                    "                        \"no\": \"DS00004421041900004\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_yigui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905697000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"581519979076997123\",\n" +
                    "                        \"no\": \"DS00004421041900002\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_chugui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905697000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"581519979081191427\",\n" +
                    "                        \"no\": \"DS00004421041900005\",\n" +
                    "                        \"brand\": \"brand_oboni\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_mumen\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905697000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"580139144344064000\",\n" +
                    "                \"salesmanId\": \"1197034592322715648\",\n" +
                    "                \"salesman\": \"水水水\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"580139144339869699\",\n" +
                    "                \"customerName\": \"梦幻大道\",\n" +
                    "                \"customerAddress\": \"阿尔巴尼亚爱尔巴桑111111111111111122222\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905696000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"580139144344064006\",\n" +
                    "                        \"no\": \"DS00004421041600003\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_yigui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905696000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"580139144344064002\",\n" +
                    "                        \"no\": \"DS00004421041600001\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_chugui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905696000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"580024218162122757\",\n" +
                    "                \"salesmanId\": \"1197034592322715648\",\n" +
                    "                \"salesman\": \"水水水\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"580024218162122754\",\n" +
                    "                \"customerName\": \"测试四十四\",\n" +
                    "                \"customerAddress\": \"天津市市辖区河东区大直沽街道111111\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905696000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"580072365924110337\",\n" +
                    "                        \"no\": \"DS00004421041500008\",\n" +
                    "                        \"brand\": \"brand_oboni\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_mumen\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_liangchi\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_liangchi_tu\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618475201000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"580072365919916035\",\n" +
                    "                        \"no\": \"DS00004421041500007\",\n" +
                    "                        \"brand\": \"brand_oboli\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_yigui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_liangchi\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_liangchi_tu\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618475201000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"580072365907333120\",\n" +
                    "                        \"no\": \"DS00004421041500006\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_chugui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618469329000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"580024218166317057\",\n" +
                    "                        \"no\": \"DS00004421041500005\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_yigui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905696000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"580024218141151236\",\n" +
                    "                \"salesmanId\": \"1197034592322715648\",\n" +
                    "                \"salesman\": \"水水水\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"580024218141151233\",\n" +
                    "                \"customerName\": \"四十四\",\n" +
                    "                \"customerAddress\": \"天津市市辖区南开区兴南街道111111111111111111\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905696000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"580024218145345537\",\n" +
                    "                        \"no\": \"DS00004421041500004\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_yigui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_hetongqianding\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905696000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"579835359847600129\",\n" +
                    "                \"salesmanId\": \"1197034592322715648\",\n" +
                    "                \"salesman\": \"水水水\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"579835359843405829\",\n" +
                    "                \"customerName\": \"嘶嘶声\",\n" +
                    "                \"customerAddress\": \"河北省唐山市古冶区唐家庄街道11111111111111\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905696000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"579835359851794433\",\n" +
                    "                        \"no\": \"DS00004421041500003\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_yigui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905696000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"579053153478856706\",\n" +
                    "                \"salesmanId\": \"1197034592322715648\",\n" +
                    "                \"salesman\": \"水水水\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"579053153474662405\",\n" +
                    "                \"customerName\": \"测试\",\n" +
                    "                \"customerAddress\": \"河北省唐山市路南区友谊街道11111111\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905696000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"579053153478856708\",\n" +
                    "                        \"no\": \"DS00004421041200015\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_chugui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_hetongqianding\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618361689000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"579002190432915458\",\n" +
                    "                \"salesmanId\": \"1197034592322715648\",\n" +
                    "                \"salesman\": \"水水水\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"579002190428721152\",\n" +
                    "                \"customerName\": \"测试实施\",\n" +
                    "                \"customerAddress\": \"天津市市辖区和平区小白楼街道11111222\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905696000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"579002190437109761\",\n" +
                    "                        \"no\": \"DS00004421041200007\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_weiyu\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_liangchi\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_liangchi_tu\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905696000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"579002190437109763\",\n" +
                    "                        \"no\": \"DS00004421041200008\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_yigui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_liangchi\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_liangchi_tu\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905696000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    },\n" +
                    "                    {\n" +
                    "                        \"id\": \"579002190432915460\",\n" +
                    "                        \"no\": \"DS00004421041200006\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_chugui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618905696000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"578996394454241281\",\n" +
                    "                \"salesmanId\": \"1197034592322715648\",\n" +
                    "                \"salesman\": \"水水水\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"578996394450046976\",\n" +
                    "                \"customerName\": \"四十四\",\n" +
                    "                \"customerAddress\": \"河北省唐山市路南区友谊街道11111\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905696000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"578996394454241283\",\n" +
                    "                        \"no\": \"DS00004421041200003\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_chugui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_liangchi\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_liangchi_tu\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1618213875000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"546832106960707585\",\n" +
                    "                \"salesmanId\": \"1159290456756977664\",\n" +
                    "                \"salesman\": \"胡立强\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"546832106956513281\",\n" +
                    "                \"customerName\": \"测试11\",\n" +
                    "                \"customerAddress\": \"河北省秦皇岛市山海关区西关街道111\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905695000,\n" +
                    "                \"orderList\": null\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"id\": \"546733264844079106\",\n" +
                    "                \"salesmanId\": \"1159290456756977664\",\n" +
                    "                \"salesman\": \"胡立强\",\n" +
                    "                \"salesmanTel\": \"18814185169\",\n" +
                    "                \"customerId\": \"546733264844079104\",\n" +
                    "                \"customerName\": \"测试\",\n" +
                    "                \"customerAddress\": \"广东省广州市荔湾区桥中街道1111\",\n" +
                    "                \"sourceChannel\": null,\n" +
                    "                \"orgId\": \"c2a6fcf3ba7342729fd0c2c20e420526\",\n" +
                    "                \"orgNo\": \"S000044\",\n" +
                    "                \"orgName\": \"测试商场ot\",\n" +
                    "                \"shopId\": \"c37e2d6b5a644a148bc32c3f62ad8298\",\n" +
                    "                \"shopNo\": \"LS00004400432\",\n" +
                    "                \"shopName\": \"测试商城——孤星\",\n" +
                    "                \"shopAddress\": \"测试\",\n" +
                    "                \"shopLocationAddress\": \"广东省广州市海珠区琶洲街道珠江\",\n" +
                    "                \"lng\": \"113.372984\",\n" +
                    "                \"lat\": \"23.106842\",\n" +
                    "                \"lastUpdateDt\": 1618905695000,\n" +
                    "                \"orderList\": [\n" +
                    "                    {\n" +
                    "                        \"id\": \"546733264848273408\",\n" +
                    "                        \"no\": \"DS00004421011300015\",\n" +
                    "                        \"brand\": \"brand_oppein\",\n" +
                    "                        \"brandName\": null,\n" +
                    "                        \"category\": \"category_chugui\",\n" +
                    "                        \"categoryName\": null,\n" +
                    "                        \"workflowNodeStatus\": \"n_dingdanxiadan\",\n" +
                    "                        \"workflowNodeStatusCustom\": \"n_hetongqianding\",\n" +
                    "                        \"contractConfirmStatus\": null,\n" +
                    "                        \"lastUpdateDt\": 1610524022000,\n" +
                    "                        \"contractDeliverDt\": null\n" +
                    "                    }\n" +
                    "                ]\n" +
                    "            }\n" +
                    "        ],\n" +
                    "        \"total\": \"93\",\n" +
                    "        \"size\": \"10\",\n" +
                    "        \"current\": \"1\",\n" +
                    "        \"pages\": \"10\"\n" +
                    "    }\n" +
                    "}";
            byte[] result = CompressUtil.compress(inputString.getBytes());
            System.out.println(result.length);
            System.out.println(inputString.length());
            byte[] deResult = CompressUtil.decompress(result);


            Assert.assertEquals(new String(deResult), inputString);

        } catch (java.io.UnsupportedEncodingException ex) {
            // handle
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
