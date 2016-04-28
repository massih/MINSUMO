package com.minsumo;




public class Main {
    private static final int SUMO_PORT = 12345;


    public static void main(String[] args) {
        try {
            DataHandler obj = new DataHandler(SUMO_PORT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}