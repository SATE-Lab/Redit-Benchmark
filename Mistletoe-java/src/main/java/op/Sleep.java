package op;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class Sleep {
    public static  CompilationUnit addSleep(CompilationUnit cu){
        return addSleep(cu,1000);
    }

    public static CompilationUnit addSleep(CompilationUnit cu,int n){
        ReentrantLock lock=new ReentrantLock();
        try {
            ClassOrInterfaceDeclaration hw = cu.getClassByName("hw").get();

            List<MethodDeclaration> x= hw.getMethods();
            String st = "Thread.sleep("+n+");//add by Mistletoe";
            for(MethodDeclaration md:x){
                System.out.println(md.getName());
                md.getBody().get()
                        .addStatement(0,StaticJavaParser.parseStatement(st));
                System.out.println(md);
            }
            return cu;
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }
}
