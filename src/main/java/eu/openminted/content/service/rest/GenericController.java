package eu.openminted.content.service.rest;

import eu.openminted.content.service.exception.ResourceNotFoundException;
import eu.openminted.content.service.exception.ServerError;
import eu.openminted.content.service.exception.ServiceAuthenticationException;
import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice
public class GenericController {
    private Logger logger = Logger.getLogger(GenericController.class);


    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(ResourceNotFoundException.class)
    @ResponseBody
    ServerError handleBadRequest(HttpServletRequest req, Exception ex) {
        logger.info(ex);
        return new ServerError(req.getRequestURL().toString(),ex);
    }

    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ExceptionHandler(ServiceAuthenticationException.class)
    @ResponseBody
    ServerError handleUnauthorizedRequest(HttpServletRequest req, Exception ex) {
        logger.info(ex);
        return new ServerError(req.getRequestURL().toString(),ex);
    }

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(Throwable.class)
    @ResponseBody
    ServerError handleBadThings(HttpServletRequest req, Exception ex) {
        logger.fatal(ex);
        return new ServerError(req.getRequestURL().toString(),ex);
    }
}
