package dominus.web.rest.endpoint;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;


/**
 * http://naleid.com/blog/2013/10/17/embedding-a-groovy-web-console-in-a-java-spring-app
 * curl --data "script=33*3" http://localhost:8090/rest/script
 * curl --data "script=ctx.getBean('rocksBean').load()" http://localhost:8090/rest/script
 */
@Produces(MediaType.APPLICATION_JSON)
//@Consumes(MediaType.TEXT_PLAIN)
@Path("script")
@Component("groovy-script")
public class GroovyScriptEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(GroovyScriptEndpoint.class);

    @Autowired
    ApplicationContext context;

    @POST
    public Map executeScript(@FormParam("script") String script) {
        LOG.info("Executing Script: " + script);
        return eval(script);
    }

    protected Map eval(String script) {
        Map resultMap = new HashMap();
        resultMap.put("script", script);
        resultMap.put("startTime", DateTime.now().toString());
        try {
            HashMap bindingValues = new HashMap();
            //TODO binding more values
            // bindingValues.put("entityManager", entityManager);
            bindingValues.put("ctx", context);
            bindingValues.put("LOG", LOG);
            GroovyShell shell = new GroovyShell(this.getClass().getClassLoader(), new Binding(bindingValues));

            Object result = shell.evaluate(script);
            String resultString = result != null ? result.toString() : "null";
            LOG.info("eval({}) result:{}", script, resultString);

            resultMap.put("result", resultString);
        } catch (Throwable t) {
            t.printStackTrace();
            LOG.error(t.toString());
            resultMap.put("error", String.format("%s [message]:%s", t.getClass().getName(), t.getMessage()));
        }

        //TODO capture stdout
        //resultMap.put("output", outputCollector.getStringBuffer().toString().trim());
        resultMap.put("endTime", DateTime.now().toString());
        return resultMap;
    }
}
