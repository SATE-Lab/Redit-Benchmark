package main;

import bean.LinkedList;

public class Test {
    public static void main(String[] args) throws Exception {
        LinkedList list = new LinkedList();
        list.pushFront("test1");
        list.pushFront("test2");
        list.pushBack( "test3");
        System.out.println(list.peekFront());
        System.out.println(list.peekBack());
        System.out.println(list.getSize());
        System.out.println(list.popBack());
        System.out.println(list.getSize());
        System.out.println(list.popFront());
        System.out.println(list.getSize());
    }
}
