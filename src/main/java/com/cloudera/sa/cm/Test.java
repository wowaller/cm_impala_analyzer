package com.cloudera.sa.cm;

public class Test {
    public static void main(String[] args) throws Exception {
        String test = "insert into table test1\r\nselect 'test', a.*,\r\n(case when test is null then 1 else 0 end) ccc\r\nfrom\r\n(select * from test2\r\n) a";
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
