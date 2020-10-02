package eu.neclab.ngsildbroker.commons.swaggerConfig;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;



@Configuration
@EnableSwagger2
public class SwaggerConfigDetails {
	
    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("eu.neclab.ngsildbroker"))
                .paths(PathSelectors.any())
                .build()
                .apiInfo(getApiInformation() );
    }

    private ApiInfo getApiInformation(){
        return new ApiInfo("Scorpio Broker APIs",
                "Description of CRUD operations",
                "1.0",
                "API Terms of Service URL",
                new Contact("GitHub", AppConstants.SWAGGER_WEBSITE_LINK, AppConstants.SWAGGER_CONTACT_LINK),
                "API License",
                "API License URL",
                Collections.emptyList()
                );
    }
}

