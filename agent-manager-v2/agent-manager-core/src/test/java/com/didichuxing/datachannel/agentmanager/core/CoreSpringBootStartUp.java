package com.didichuxing.datachannel.agentmanager.core;

import com.didichuxing.datachannel.agentmanager.thirdpart.agent.metrics.AgentMetricsDAO;
import com.didichuxing.datachannel.agentmanager.thirdpart.agent.metrics.impl.AgentMetricsRDSImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(scanBasePackages = { "com.didichuxing.datachannel.agentmanager" })
public class CoreSpringBootStartUp {
    public static void main(String[] args) {
        SpringApplication.run(CoreSpringBootStartUp.class, args);
    }

    @Bean
    public AgentMetricsDAO agentMetricsDAO() {
        return new AgentMetricsRDSImpl();
    }
}
