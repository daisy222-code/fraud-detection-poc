package com.example.fraud;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.concurrent.TimeUnit;

@Component
public class FlinkEngine {
    private JobClient job;

    @PostConstruct
    public void start() throws Exception {
        Configuration config = new Configuration();
        config.setString("rest.bind-address", "127.0.0.1");
        config.setString("rest.bind-port", "0");
        config.setString("jobmanager.bind-host", "127.0.0.1");
        config.setString("taskmanager.bind-host", "127.0.0.1");
        config.setString("restart-strategy.type", "none");
        var env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
        env.addSource(new QueueSource()).name("Local transaction queue")
                .keyBy(t -> t.accountId).process(new FraudDetector()).name("Account fraud rules")
                .addSink(new AlertSink()).name("Recent alerts");
        job = env.executeAsync("Real-time fraud detection POC");
    }

    public String status() {
        try {
            return job == null ? "STARTING" : job.getJobStatus().get(2, TimeUnit.SECONDS).name();
        } catch (Exception e) {
            return "UNAVAILABLE";
        }
    }

    @PreDestroy
    public void stop() throws Exception {
        if (job != null) job.cancel().get(10, TimeUnit.SECONDS);
    }

    public static class QueueSource extends RichParallelSourceFunction<Transaction> {
        private volatile boolean running = true;

        public void run(SourceContext<Transaction> ctx) throws Exception {
            while (running) {
                var t = LocalBridge.INPUT.poll(200, TimeUnit.MILLISECONDS);
                if (t != null) synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(t);
                }
            }
        }

        public void cancel() {
            running = false;
        }
    }

    public static class AlertSink implements SinkFunction<Alert> {
        @Override
        public void invoke(Alert a, Context ctx) {
            LocalBridge.add(a);
        }
    }
}
