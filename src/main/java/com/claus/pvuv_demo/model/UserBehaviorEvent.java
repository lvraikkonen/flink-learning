package com.claus.pvuv_demo.model;

import lombok.*;

@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class UserBehaviorEvent {
    private Integer userId;
    private Integer itemId;
    private String category;
    private String clientIP;
    private String action;
    private Long ts;
}
