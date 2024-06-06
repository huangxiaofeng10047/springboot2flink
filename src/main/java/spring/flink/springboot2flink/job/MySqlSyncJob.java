package spring.flink.springboot2flink.job;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcRowOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

// Flink作业实现MySQL数据同步
public class MySqlSyncJob {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 配置MySQL源（source）
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
            .setDrivername("com.mysql.cj.jdbc.Driver")
            .setDBUrl("jdbc:mysql://localhost:3309/test")
            .setUsername("root")
            .setPassword("123456")
            .setQuery("select * from user")
            .setRowTypeInfo(new RowTypeInfo(Types.LONG, Types.STRING, Types.INT, Types.INT, Types.STRING))
            .finish();

        // 配置MySQL目标（sink）
        JdbcRowOutputFormat jdbcOutputFormat = JdbcRowOutputFormat.buildJdbcOutputFormat()
            .setDrivername("com.mysql.cj.jdbc.Driver")
            .setDBUrl("jdbc:mysql://localhost:3309/test")
            .setUsername("root")
            .setPassword("123456")
            .setQuery("insert into user2 (id, name, age,sex,phone) values (?, ?, ?, ?, ?)")
            .finish();

        // 创建数据源
        DataStreamSource<Row> source = env.createInput(jdbcInputFormat);

        // 写入数据到目标数据库
        source.writeUsingOutputFormat(jdbcOutputFormat);

        // 执行作业
        env.execute("MySQL Data Sync Job");
    }
}

