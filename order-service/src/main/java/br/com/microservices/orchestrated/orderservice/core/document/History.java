package br.com.microservices.orchestrated.orderservice.core.document;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class History {

    private String source;
    private String status;
    private String message;
    private LocalDateTime createdAt;
}
