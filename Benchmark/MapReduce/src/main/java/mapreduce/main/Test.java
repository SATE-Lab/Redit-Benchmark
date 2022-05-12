package mapreduce.main;

public class Test {
    public static void main(String[] args) throws Exception {
//        LinkedList list = new LinkedList();
//        list.pushFront("test1");
//        list.pushFront("test2");
//        list.pushBack( "test3");
//        System.out.println(list.peekFront());
//        System.out.println(list.peekBack());
//        System.out.println(list.getSize());
//        System.out.println(list.popBack());
//        System.out.println(list.getSize());
//        System.out.println(list.popFront());
//        System.out.println(list.getSize());
//
//        System.out.println(System.currentTimeMillis()/1000);
//        System.out.println(formatCurTime(getNowTimeSecond() * 1000));

//        createAllDir();

        String a = "apple";
        String aa = "apples";
        String b = "code";
        String bb = "cake";
        String c = "ssss";
        String d = " ";
        String e = "1";
        System.out.println(a.hashCode() + " " + a.hashCode() % 10);
        System.out.println(aa.hashCode() + " " + Math.abs(aa.hashCode() % 10));
        System.out.println(b.hashCode() + " " + b.hashCode() % 10);
        System.out.println(bb.hashCode() + " " + bb.hashCode() % 10);
        System.out.println(c.hashCode() + " " + c.hashCode() % 10);
        System.out.println(d.hashCode() + " " + d.hashCode() % 10);
        System.out.println(e.hashCode() + " " + e.hashCode() % 10);
    }

}
