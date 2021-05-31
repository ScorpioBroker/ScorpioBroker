package eu.neclab.ngsildbroker.entityhandler.config;


import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import com.zaxxer.hikari.HikariDataSource;

import eu.neclab.ngsildbroker.commons.tenant.TenantAwareDataSource;



@Configuration
public class EntityJdbcConfig {
		
	// https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#howto-two-datasources

    @Bean
    @Primary
    @ConfigurationProperties(prefix = "reader.datasource")
    public DataSourceProperties getReaderDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "readerDataSource")
    @Primary
    @ConfigurationProperties(prefix = "reader.datasource.hikari")
    public DataSource getReaderDataSource() {
        return getReaderDataSourceProperties().initializeDataSourceBuilder()
                .type(HikariDataSource.class).build();
    }

    @Bean(name = "readerJdbcTemplate")
    public JdbcTemplate getReaderJdbcTemplate() {
    	return new JdbcTemplate(getReaderDataSource());
    }
	

    @Bean
    @ConfigurationProperties(prefix = "writer.datasource")
    public DataSourceProperties getWriterDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "writerDataSource")
    @ConfigurationProperties(prefix = "writer.datasource.hikari")
    public DataSource getWriterDataSource() {
        return getWriterDataSourceProperties().initializeDataSourceBuilder()
                .type(HikariDataSource.class).build();
    }

    @Bean(name = "writerJdbcTemplate")
    public JdbcTemplate getWriterJdbcTemplate() {
    	return new JdbcTemplate(getWriterDataSource());
    }
    
    @Bean(name = "tenantAwareDataSource")	
	public DataSource tenantAwareDataSource() {
		return new TenantAwareDataSource();
	}

}
