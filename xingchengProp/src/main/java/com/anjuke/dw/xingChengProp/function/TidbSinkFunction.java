package com.anjuke.dw.xingChengProp.function;

import com.anjuke.dw.xingChengProp.DTO.XingchengPropDto;
import com.anjuke.dw.xingChengProp.util.EnvironmentConfiguration;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Objects;

public class TidbSinkFunction extends RichSinkFunction<XingchengPropDto> {

    EnvironmentConfiguration envConf ;
    private Connection conn ;
    private PreparedStatement insertStmt ;

    Object obj ;
    //连接池对象
    ComboPooledDataSource jdbcurlDataSource ;
    ArrayList<XingchengPropDto> listBuffer ;

    private String URL;
    private String USER ;
    private String PASSWORD ;

    public TidbSinkFunction(EnvironmentConfiguration envConf) {
        this.envConf = envConf;
    }

    //private String URL = "jdbc:mysql://qfolapdb.tdb.58dns.org:26700/tdb58_qf_olap_db?charset=utf8&rewriteBatchedStatements=true&allowMultiQueries=true&autoReconnect=true";
    //private String USER = "qfolap_yc";
    //private String PASSWORD = "348827216c7375d8";

    //private String URL = "jdbc:mysql://10.167.200.134:26700/tdb58_qf_olap_db?useUnicode=true&charset=utf8&rewriteBatchedStatements=true&allowMultiQueries=true&autoReconnect=true";
    //private String USER = "qfolap_yc";
    //private String PASSWORD = "348827216c7375d8";

    //private String URL = "jdbc:mysql://localhost:3306/test_db?characterEncoding=utf8&rewriteBatchedStatements=true";
    //private String USER = "root";
    //private String PASSWORD = "Root123456";

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

    @Override
    public void open(Configuration parameters) throws Exception {
         obj = new Object();
        listBuffer = new ArrayList<XingchengPropDto>();
        super.open(parameters);
        System.out.println("jdbc 链接 初始化 end1" );

       // Class.forName("com.mysql.cj.jdbc.Driver");
       // conn = DriverManager.getConnection(URL, USER, PASSWORD);
        //System.out.println("conn");
        //System.out.println(conn);


        System.out.println("jdbc 链接 初始化 2" );
        URL = envConf.getTidb_url();
        System.out.println("URL:");
        System.out.println(URL);
        USER = envConf.getTidb_username();
        System.out.println("USER:");
        System.out.println(USER);
        PASSWORD = envConf.getTidb_password();
        System.out.println("PASSWORD:");
        System.out.println(PASSWORD);

        conn = getConnection();
        //System.out.println("conn:"+ conn );
        conn.setAutoCommit(false);

        insertStmt = conn.prepareStatement(sql);
        System.out.println("jdbc 链接 初始化 done" );

    }

    //从连接池中抓起链接 conn
   public Connection  getConnection() throws PropertyVetoException, SQLException {
       synchronized (obj) {
           if(Objects.isNull(jdbcurlDataSource)) {
               jdbcurlDataSource = getJDBCURLDataSource(URL, USER, PASSWORD);
           }
       }
        return jdbcurlDataSource.getConnection();
   }
   // 构造链接池
   public ComboPooledDataSource getJDBCURLDataSource(String url ,String user, String password ) throws PropertyVetoException {
       ComboPooledDataSource cpds  = new ComboPooledDataSource();

       cpds.setDriverClass("com.mysql.cj.jdbc.Driver");
       cpds.setJdbcUrl(url);
       cpds.setUser(user);
       cpds.setPassword(password);

       //cpds.setInitialPoolSize(1)
       cpds.setMinPoolSize(3);
       cpds.setMaxPoolSize(40);
       cpds.setAcquireIncrement(4);
       cpds.setMaxIdleTime(600);

       cpds.setIdleConnectionTestPeriod(300);
       cpds.setPreferredTestQuery("SELECT 1");

       return  cpds;

   }


    @Override
    public void invoke(XingchengPropDto value, Context context) throws Exception {
         listBuffer.add(value);
        System.out.println("-------------------------执行sql---------------------------");
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
// --------insert -------------
   // update column
        insertStmt.setInt(33, value.getProperty_add_num());
        insertStmt.setInt(34, value.getProperty_activate_num());
        insertStmt.setInt(35, value.getKey_num());
        insertStmt.setInt(36, value.getProp_follow_num());
        insertStmt.setInt(37, value.getRecord_num());
        insertStmt.addBatch();

      //  System.out.println(insertStmt.toString());
        System.out.println("-------------------------执行sql 2---------------------------");
        if(listBuffer.size() >= 5 ) {
            insertStmt.executeBatch();
            conn.commit();
            listBuffer.clear();
        }
        System.out.println("-------------------------执行sql 3---------------------------");
    }
    // 关闭时做清理工作
    @Override
    public void close() throws Exception {
        super.close();
        if(insertStmt != null) {
            insertStmt.close();
        }
        if(conn != null) {
            //conn.rollback();
            conn.close();
        }
        System.out.println("jdbc 链接关闭 end ");
    }
}
