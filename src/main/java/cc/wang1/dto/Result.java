package cc.wang1.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Result {
    private boolean result;
    private String message;
    private LocalDateTime deliveredTime;
    private LocalDateTime touchedTime;
    private long touchedTimestamp;
    private LocalDateTime expire;
    private long expireTimestamp;
}
