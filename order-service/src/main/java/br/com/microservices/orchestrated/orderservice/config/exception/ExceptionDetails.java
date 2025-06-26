package br.com.microservices.orchestrated.orderservice.config.exception;

import java.io.Serializable;

public record ExceptionDetails(int status, String message)  {

}
