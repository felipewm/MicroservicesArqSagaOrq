package br.com.microservices.orchestrated.productvalidationservice.core.service;

import br.com.microservices.orchestrated.productvalidationservice.core.dto.History;
import br.com.microservices.orchestrated.productvalidationservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.productvalidationservice.core.dto.Event;
import br.com.microservices.orchestrated.productvalidationservice.core.dto.OrderProducts;
import br.com.microservices.orchestrated.productvalidationservice.core.model.Validation;
import br.com.microservices.orchestrated.productvalidationservice.core.producer.KafkaProducer;
import br.com.microservices.orchestrated.productvalidationservice.core.repository.ProductRepository;
import br.com.microservices.orchestrated.productvalidationservice.core.repository.ValidationRepository;
import br.com.microservices.orchestrated.productvalidationservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static br.com.microservices.orchestrated.productvalidationservice.core.enums.ESagaStatus.*;
import static org.springframework.util.ObjectUtils.isEmpty;

@Slf4j
@Service
@AllArgsConstructor
public class ProductValidationService {

    private static final String CURRENT_SOURCE = "PRODUCT_VALIDATION_SERVICE";

    private final JsonUtil jsonUtil;
    private final KafkaProducer kafkaProducer;
    private final ValidationRepository validationRepository;
    private final ProductRepository productRepository;

    public void validateExistingProducts(Event event ){
        try {
            checkCurrentValidation(event );
            createValidation(event, true);
            handleSuccess(event);
        }catch (Exception e){
            log.error("Error trying to validate products: {0}", e);
            handleFailCurrentNotExecuted(event, e.getMessage());
        }

        kafkaProducer.sendEvent(jsonUtil.toJson(event));
    }

    private void checkCurrentValidation(Event event) {
            validateProductsInformed(event);
        if (validationRepository.existsByOrderIdAndTransactionId(
                event.getOrderId(), event.getTransactionId())) {
            throw new ValidationException("There is another transactionId for this validation!");
        }
        event.getPayload().getProducts().forEach(product -> {
            validateProductInformed(product);
            validateExistingProduct(product.getProduct().getCode());
        });
    }

    private void validateProductInformed(OrderProducts products){
        if(isEmpty(products.getProduct()) || isEmpty(products.getProduct().getCode())){
            throw new ValidationException("Product must be informed!!");
        }
    }

    private void validateExistingProduct(String code){
        if(!productRepository.existsByCode(code)){
            throw new ValidationException("Product with code " + code + " does not exist in database!");
        }
    }

    private void createValidation(Event event, boolean success) {
        var validation = Validation
                .builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .success(success)
                .build();

        validationRepository.save(validation);
    }

    private void validateProductsInformed(Event event){
        if(isEmpty(event.getPayload()) || isEmpty(event.getPayload().getProducts())){
            throw new ValidationException("Product list is empty!");
        }
        if(isEmpty(event.getPayload().getId()) || isEmpty(event.getPayload().getTransactionId())){
            throw new ValidationException("OrderId and TransactionId must be informed!");
        }
    }


    private void handleSuccess(Event event) {
        event.setStatus(SUCCESS);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Products are validated successfully!");

    }

    private void addHistory(Event event, String message){
        var history = History
                .builder()
                .source(event.getSource())
                .status(event.getStatus())
                .message(message)
                .createdAt(LocalDateTime.now())
                .build();

        event.addToHistory(history);
    }

    private void handleFailCurrentNotExecuted(Event event, String message) {

        event.setStatus(ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Fail to validate products: !".concat(message));
    }

    public void rollbackEvent(Event event){
        changeValidationtoFail(event);
        event.setStatus(FAIL);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Rollback executed no product-validation!");
        kafkaProducer.sendEvent(jsonUtil.toJson(event));

    }

    private void changeValidationtoFail(Event event){

        validationRepository.findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
        .ifPresentOrElse(validation -> {
            validation.setSuccess(false);
            validationRepository.save(validation);
        }, () -> createValidation(event, false));
    }




}
