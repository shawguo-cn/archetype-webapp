package dominus.web.rest.endpoint;

import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

@Component
@Path("/hello")
public class HelloWorldEndpoint {

    //http://localhost:8080/hello/world?var=shawguo
    @GET
    @Path("/world")
    public String test(@QueryParam("var") String var) {
        return String.format("Hello world! %s", var);
    }
}
