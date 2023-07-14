package org.DFSdemo;

import org.DFSdemo.conf.Configuration;

import java.io.IOException;
import java.net.URI;

public class App {
    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();

        NamenodeClient namenodeClient = new NamenodeClient(URI.create(("namenode://localhost:8866")), conf);
        boolean isSuccessful = namenodeClient.rename2("testSrc", "testDst");
        System.out.println(isSuccessful);
        namenodeClient.close();
    }
}
