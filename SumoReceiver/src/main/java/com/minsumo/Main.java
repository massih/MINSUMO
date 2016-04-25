package com.minsumo;




public class Main {

    public static void main(String[] args) {
        try {
            DataHandler obj = new DataHandler(12345);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}