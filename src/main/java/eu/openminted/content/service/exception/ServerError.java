package eu.openminted.content.service.exception;

public class ServerError {

    public final String url;
    public final String error;

    public ServerError(String url, Exception ex) {
        this.url = url;
        this.error = ex.getMessage();
    }
}
