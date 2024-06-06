package spring.flink.springboot2flink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import spring.flink.springboot2flink.job.FlinkJobService;

@SpringBootApplication
public class DataSyncApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(DataSyncApplication.class, args);
        FlinkJobService flinkJobService = context.getBean(FlinkJobService.class);

        try {
            flinkJobService.runFlinkJob();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

