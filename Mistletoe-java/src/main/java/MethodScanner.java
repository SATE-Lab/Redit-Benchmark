import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.ThisExpr;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.type.Type;
import op.Lock;
import op.Sleep;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class MethodScanner {

    public static void main(String[] args) throws FileNotFoundException {
        String filePath = System.getProperty("user.dir") + "/Mistletoe-java/src/main/resources/Coordinator.java";
        String className = "Coordinator";
        System.out.println(filePath);
        MethodScanner ms = new MethodScanner();
        System.out.println("——————————Scan Start——————————");
//        ms.scanTest(filePath, className);
        ms.checkLock(filePath, className);
        System.out.println("——————————Scan End——————————");
        CompilationUnit cu = StaticJavaParser.parse(new File(filePath));

//        cu = Sleep.addSleep(cu, className, 300);
//        cu = Lock.addLock(cu, className);

        FileWriter writer;
        try {
            writer = new FileWriter(filePath);
            assert cu != null;
            writer.write(cu.toString());
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void scanTest(String filePath, String className) throws FileNotFoundException{
        CompilationUnit cu = StaticJavaParser.parse(new File(filePath));
        ClassOrInterfaceDeclaration hw = cu.getClassByName(className).get();
        List<MethodDeclaration> x = hw.getMethods();
        for(MethodDeclaration md:x){
            System.out.println(md.getName());
        }
    }

    private void checkLock(String filePath, String className) throws FileNotFoundException {
        CompilationUnit cu = StaticJavaParser.parse(new File(filePath));
        ClassOrInterfaceDeclaration hw = cu.getClassByName(className).get();
        List<FieldDeclaration> fieldDeclarations = hw.getFields();
        for(FieldDeclaration fd : fieldDeclarations){
            if (fd.getElementType().toString().equals("ReentrantLock"))
                System.out.println(fd.getElementType() + ", find ReentrantLock...");
        }
    }


    private void bookTest() throws FileNotFoundException{
        CompilationUnit cu = StaticJavaParser.parse(new File(""));
        ClassOrInterfaceDeclaration book = cu.addClass("Book");

        book.addField("String", "title");
        book.addField("Person", "author");

        book.addConstructor(Modifier.Keyword.PUBLIC)
                .addParameter("String", "title")
                .addParameter("Person", "author")
                .setBody(new BlockStmt()
                        .addStatement(new ExpressionStmt(new AssignExpr(
                                new FieldAccessExpr(new ThisExpr(), "title"),
                                new NameExpr("title"),
                                AssignExpr.Operator.ASSIGN)))
                        .addStatement(new ExpressionStmt(new AssignExpr(
                                new FieldAccessExpr(new ThisExpr(), "author"),
                                new NameExpr("author"),
                                AssignExpr.Operator.ASSIGN))));

        book.addMethod("getTitle", Modifier.Keyword.PUBLIC).setBody(
                new BlockStmt().addStatement(new ReturnStmt(new NameExpr("title"))));

        book.addMethod("getAuthor", Modifier.Keyword.PUBLIC).setBody(
                new BlockStmt().addStatement(new ReturnStmt(new NameExpr("author"))));

        System.out.println(cu.toString());
    }

}

