package dominus.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@ComponentScan(basePackages = "gladiator")
@PropertySource("classpath:properties/${properties.profile}/data-store.properties")
public class GlobalConfig {

    @Autowired
    protected Environment env;
}
