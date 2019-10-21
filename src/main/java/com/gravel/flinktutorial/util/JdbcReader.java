package com.gravel.flinktutorial.util;

import com.gravel.flinktutorial.domain.Students;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Gravel
 * @date 2019/10/21.
 */
public class JdbcReader extends RichSourceFunction<Students> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://142.4.211.198:3306/canal_test", "canal", "canal");
        ps = connection.prepareStatement("select * from test");
    }

    //执行查询并获取结果
    @Override
    public void run(SourceContext<Students> ctx) throws Exception {
        while (isRunning) {
            try {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    Students student = Students.builder().id(resultSet.getLong(1)).name(resultSet.getString(3))
                            .age(resultSet.getInt(2)).className(resultSet.getString(4)).build();
                    ctx.collect(student);
                }
            } catch (Exception e) {
                logger.error("runException:{}", e);
            }
            Thread.sleep(5000);
        }
    }

    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}
