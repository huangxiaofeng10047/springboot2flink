package spring.flink.springboot2flink.job;

import org.springframework.stereotype.Service;

/**
 * @author hxf16
 */
@Service
public class FlinkJobService {

    public void runFlinkJob() throws Exception {
        MySqlSyncJob.main(new String[]{});
    }
}

