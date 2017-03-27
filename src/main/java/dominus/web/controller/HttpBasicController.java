package dominus.web.controller;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

@RestController
public class HttpBasicController {

    @RequestMapping("/basic")
    @ResponseBody
    String home(HttpServletRequest request) {

        //EE: Cookie
        if (request.getCookies() != null) {
            for (Cookie cookie : request.getCookies()) {
                System.out.println(cookie.getName() + ":" + cookie.getValue());
            }
        } else {
            System.out.println("WTF! Cookie is null!");
        }



        return "Hello World!";
    }
}