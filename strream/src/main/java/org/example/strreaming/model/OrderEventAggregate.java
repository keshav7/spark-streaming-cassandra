package org.example.strreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderEventAggregate implements Serializable {

    private String timestamp;
    private String eventName;
    private Integer totalCount;

}
