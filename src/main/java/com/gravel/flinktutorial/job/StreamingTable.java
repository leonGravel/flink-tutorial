package com.gravel.flinktutorial.job;

import com.gravel.flinktutorial.domain.Students;
import com.gravel.flinktutorial.util.JdbcReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author Gravel
 * @date 2019/10/21.
 */
public class StreamingTable {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Students> dataStream = env.addSource(new JdbcReader());

        Table table = tableEnv.fromDataStream(dataStream);

        Table result = tableEnv.sqlQuery("SELECT  * FROM " + table);
        tableEnv.toAppendStream(result, Students.class).print();
        env.execute("Flink cost DB data to write Database");
    }
}
