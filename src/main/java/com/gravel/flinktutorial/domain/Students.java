package com.gravel.flinktutorial.domain;

import lombok.Builder;
import lombok.Data;

/**
 * @author Gravel
 * @date 2019/10/21.
 */
@Data
@Builder
public class Students {
    private Long id;
    private int age;
    private String name;
    private String className;
}
