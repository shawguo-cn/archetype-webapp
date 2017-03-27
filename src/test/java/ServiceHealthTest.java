import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ServiceHealthTest {

    @Test
    public void testServiceHealth() throws UnirestException {
        HttpResponse<String> jsonResponse = Unirest.get("http://localhost:8090/health").asString();
        assertTrue(jsonResponse.getBody().equals("OK"));
    }


}
