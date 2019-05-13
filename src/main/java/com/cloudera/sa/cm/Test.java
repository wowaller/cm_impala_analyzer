package com.cloudera.sa.cm;

public class Test {
    public static void main(String[] args) throws Exception {
        String test = "with a0 as (select * from test),\r\n a1 as (select * from a0, test1)\r\ninsert  \r\n overwrite \r\n out.tts \r\n partition(OART,testr4=2) select * from a1";
        QueryBase node = new QueryBase(test, null);
        for(String source : node.getSource()) {
            System.out.println(source);
        }

        System.out.println("Target Tables====");
        for(String target : node.getTarget()) {
            System.out.println(target);
        }
    }
}
