package eu.openminted.content.service.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.UNAUTHORIZED)
public class ServiceAuthenticationException extends RuntimeException  {
    public ServiceAuthenticationException() {
        super("User Not Authorized");
    }
}
