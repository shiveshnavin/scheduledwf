package io.github.jas34.scheduledwf;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
public class Config {


    private static final String WALLET_PATH = "D:\\code\\scheduledwf\\common-creds\\oracle\\semibit-db";
    private static final String DB_USERNAME = "semibit";
    private static final String DB_PASSWORD = "YerRahaPassword19";

    @Bean
    @Primary
    public DataSource dataSource() {
        return DataSourceBuilder.create()
                .url(buildJdbcUrl())
                .username(DB_USERNAME)
                .password(DB_PASSWORD)
                .build();
    }


    @ConditionalOnMissingBean
    @Bean
    public BuildProperties buildProperties() {
        Properties properties = new Properties();
        properties.put("group", "io.github.jas34");
        properties.put("artifact", "scheduledwf-server");
        properties.put("version", "2.0.2-SNAPSHOT");
        return new BuildProperties(properties);
    }
    private String buildJdbcUrl() {
        return String.format("jdbc:oracle:thin:@tcps://adb.ap-mumbai-1.oraclecloud.com:1522/g5c95d8822b0101_sembitdb_low.adb.oraclecloud.com?wallet_location=common-creds/oracle/semibit-db");
    }

}
