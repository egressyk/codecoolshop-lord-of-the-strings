package com.codecool.shop.exception;

public class DatabaseException extends RuntimeException{
    public DatabaseException(String message){
        super(message);
    }
}