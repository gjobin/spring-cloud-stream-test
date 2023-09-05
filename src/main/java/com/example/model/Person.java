package com.example.model;

import lombok.*;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private String firstname;
    private String lastname;
}