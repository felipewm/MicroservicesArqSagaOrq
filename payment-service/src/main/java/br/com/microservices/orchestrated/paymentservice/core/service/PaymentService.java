package br.com.microservices.orchestrated.paymentservice.core.service;


import br.com.microservices.orchestrated.paymentservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.paymentservice.core.dto.Event;
import br.com.microservices.orchestrated.paymentservice.core.dto.History;
import br.com.microservices.orchestrated.paymentservice.core.dto.OrderProducts;
import br.com.microservices.orchestrated.paymentservice.core.enums.EPaymentStatus;
import br.com.microservices.orchestrated.paymentservice.core.model.Payment;
import br.com.microservices.orchestrated.paymentservice.core.producer.KafkaProducer;
import br.com.microservices.orchestrated.paymentservice.core.repository.PaymentRepository;
import br.com.microservices.orchestrated.paymentservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static br.com.microservices.orchestrated.paymentservice.core.enums.ESagaStatus.*;


@Slf4j
@Service
@AllArgsConstructor
public class PaymentService {


    private static final String CURRENT_SOURCE = "PAYMENT_SERVICE";
    private static final Double REDUCE_SUM_VALUE = 0.0;
    private static final Double MIN_AMOUNT_VALUE = 0.1;


    private final JsonUtil jsonUtil;
    private final KafkaProducer producer;
    private final PaymentRepository paymentRepository;


    public void realizePayment(Event event) {
        try {
            checkCurrentValidation(event);
            createPendingPayment(event);
            var payment = findByOrderIdAndeTransactionId(event);
            validateAmount(payment.getTotalAmount());
            changePaymentToSuccess(payment);
            handleSuccess(event);

        } catch (Exception e) {
            log.error("Error trying to make payment: {0}", e);
            handleFailCurrentNotExecuted(event, e.getMessage());
        }

        producer.sendEvent(jsonUtil.toJson(event));
    }

    private void createPendingPayment(Event event) {

        var totalAmount = calculateAmount(event);
        var totalItens = calculatetotalItens(event);
        var payment = Payment.builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .totalAmount(totalAmount)
                .totalItens(totalItens)
                .build();

        save(payment);

        setEventAmountItens(event, payment);
    }

    private void setEventAmountItens(Event event, Payment payment) {
        event.getPayload().setTotalAmount(payment.getTotalAmount());
        event.getPayload().setTotalItems(payment.getTotalItens());
    }

    private void validateAmount(double amount) {
        if(amount < MIN_AMOUNT_VALUE) {
            throw new ValidationException("The minimum amount value is " + MIN_AMOUNT_VALUE.toString());
        }
    }

    private void changePaymentToSuccess(Payment payment) {

        payment.setStatus(EPaymentStatus.SUCCESS);
        save(payment);
    }


    private void handleSuccess(Event event) {
        event.setStatus(SUCCESS);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Payment realized successfully!");

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

    public void realizeRefound(Event event) {
        event.setStatus(FAIL);
        event.setSource(CURRENT_SOURCE);
        try {
            changePaymentStatusToRefound(event);
            addHistory(event, "Rollback executed for payment!");

        } catch (Exception e) {
            addHistory(event, "Rollback not executed for payment! ".concat(e.getMessage()));
            //throw new RuntimeException(e);
        }
        producer.sendEvent(jsonUtil.toJson(event));
    }

    private void changePaymentStatusToRefound(Event event) {
        var payment = findByOrderIdAndeTransactionId(event);
        payment.setStatus(EPaymentStatus.REFOUND);
        setEventAmountItens(event, payment);
        save(payment);

    }

    private void handleFailCurrentNotExecuted(Event event, String message) {

        event.setStatus(ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Fail to realize payment: !".concat(message));
    }

    private int calculatetotalItens(Event event) {
        return event.getPayload().getProducts().stream().map(OrderProducts::getQuantity)
                .reduce(REDUCE_SUM_VALUE.intValue(), Integer::sum);
    }

    private double calculateAmount(Event event) {
        return event.getPayload().getProducts().stream().map(product -> product.getQuantity() * product.getProduct().getUnitValue())
                .reduce(REDUCE_SUM_VALUE, Double::sum);
    }

    private Payment findByOrderIdAndeTransactionId(Event event) {
        return paymentRepository.findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
                .orElseThrow(() -> new ValidationException("Payment not found by OrderId and TransactionId!"));

    }

    private void save(Payment payment) {
        paymentRepository.save(payment);
    }

    private void checkCurrentValidation(Event event) {
        if (paymentRepository.existsByOrderIdAndTransactionId(
                event.getOrderId(), event.getTransactionId())) {
            throw new ValidationException("There is another transactionId for this validation!");
        }
    }
}