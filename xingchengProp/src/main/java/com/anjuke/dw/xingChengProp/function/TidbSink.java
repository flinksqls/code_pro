package com.anjuke.dw.xingChengProp.function;

import com.anjuke.dw.xingChengProp.DTO.XingchengPropDto;
import com.anjuke.dw.xingChengProp.util.EnvironmentConfiguration;
import com.google.errorprone.annotations.Var;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class TidbSink {
    EnvironmentConfiguration envConf ;
    JdbcExecutionOptions optionsBuild ;
    JdbcConnectionOptions connectionBuild ;
    private String sql = "INSERT INTO da_qf_prop_xingcheng_emp_real_code\n" +
            "(\n" +
            "    companyuuid\n" +
            "    ,employeeUuid\n" +
            "    ,deptUuid\n" +
            "    ,deptUuid1\n" +
            "    ,deptUuid2\n" +
            "    ,deptUuid3\n" +
            "    ,deptUuid4\n" +
            "    ,deptUuid5\n" +
            "    ,deptUuid6\n" +
            "    ,deptUuid7\n" +
            "    ,deptUuid8\n" +
            "    ,dayTime\n" +
            "    ,house_type\n" +
            "    ,property_add_num\n" +
            "    ,property_activate_num\n" +
            "    ,key_num\n" +
            "    ,survey_num\n" +
            "    ,common_entrust_num\n" +
            "    ,exclusive_entrust_num\n" +
            "    ,indemnity_entrust_num\n" +
            "    ,media_add_num\n" +
            "    ,vr_add_num\n" +
            "    ,heyan_add_num\n" +
            "    ,prop_follow_num\n" +
            "    ,prop_tel_num\n" +
            "    ,prop_tel_duration\n" +
            "    ,record_num\n" +
            "    ,cal_dt\n" +
            "    ,photoadd_num\n" +
            "    ,dept_uuid_path\n" +
            "    ,dept_uuid_path_md5\n" +
            "    ,photo_company_add_num\n" +
            "\n" +
            ") VALUES (\n" +
            "    ?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            "    ,?\n" +
            ")ON DUPLICATE KEY UPDATE \n" +
            "\n" +
            "property_add_num=?\n" +
            ",property_activate_num=?\n" +
            ",key_num=?\n"+
            ",prop_follow_num=?\n"+
            ",record_num=?\n"
            ;
    public TidbSink(EnvironmentConfiguration envConf) {
        this.envConf = envConf;
        optionsBuild = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(100)
                .withMaxRetries(3)
                .build();
        connectionBuild = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl(this.envConf.tidb_url)
                .withDriverName(this.envConf.tidb_driver)
                .withUsername(this.envConf.tidb_username)
                .withPassword(this.envConf.tidb_password)
                .build();
    }
    public SinkFunction<XingchengPropDto> getSink(){
        return JdbcSink.sink(
                sql
                ,(insertStmt,value) ->{
                    // insert column
                    insertStmt.setString(1, value.getCompanyUuid());
                    insertStmt.setString(2, value.getEmployeeUuid());
                    insertStmt.setString(3, value.getDeptUuid());
                    insertStmt.setString(4, value.getDeptUuid1());
                    insertStmt.setString(5, value.getDeptUuid2());
                    insertStmt.setString(6, value.getDeptUuid3());
                    insertStmt.setString(7, value.getDeptUuid4());
                    insertStmt.setString(8, value.getDeptUuid5());
                    insertStmt.setString(9, value.getDeptUuid6());
                    insertStmt.setString(10, value.getDeptUuid7());

                    insertStmt.setString(11, value.getDeptUuid8());

                    insertStmt.setString(12, value.getAddTime_date());
                    insertStmt.setString(13, value.getTradeKind());
                    insertStmt.setInt(14, value.getProperty_activate_num());
                    insertStmt.setInt(15, value.getProperty_add_num());
                    insertStmt.setInt(16, value.getKey_num());
                    insertStmt.setInt(17, value.getSurvey_num());
                    insertStmt.setInt(18, value.getCommon_entrust_num());
                    insertStmt.setInt(19, value.getExclusive_entrust_num());
                    insertStmt.setInt(20, value.getIndemnity_entrust_num());

                    insertStmt.setInt(21, value.getMedia_add_num());
                    insertStmt.setInt(22, value.getVr_add_num());
                    insertStmt.setInt(23, value.getHeyan_add_num());
                    insertStmt.setInt(24, value.getProp_follow_num());
                    insertStmt.setInt(25, value.getProp_tel_num());
                    insertStmt.setInt(26, value.getProp_tel_duration());
                    insertStmt.setInt(27, value.getRecord_num());
                    insertStmt.setString(28, value.getUpdateTime_date());
                    insertStmt.setInt(29, value.getPhotoadd_num());
                    insertStmt.setString(30, value.getDept_uuid_path());
                    insertStmt.setString(31, value.getDept_uuid_substr_concat());
                    insertStmt.setInt(32, value.getPhoto_company_add_num());

                    // update column
                    insertStmt.setInt(33, value.getProperty_add_num());
                    insertStmt.setInt(34, value.getProperty_activate_num());
                    insertStmt.setInt(35, value.getKey_num());
                    insertStmt.setInt(36, value.getProp_follow_num());
                    insertStmt.setInt(37, value.getRecord_num());
                   // System.out.println("sql 执行!!!");
                }
                ,optionsBuild
                ,connectionBuild

        );

    }




}
