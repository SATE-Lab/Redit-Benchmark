import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.ThisExpr;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;
import com.github.javaparser.ast.Modifier;
import op.Sleep;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class MethodScanner {

    public static void main(String[] args) throws FileNotFoundException {
        System.out.println(getPath());
        MethodScanner ms=new MethodScanner();
        System.out.println("——————————Scan Start——————————");
        ms.scanTest();
        System.out.println("——————————Scan End——————————");
        CompilationUnit cu = StaticJavaParser.parse(new File(getPath()));
        cu= Sleep.addSleep(cu,300);
        FileWriter writer;
        try {
            writer = new FileWriter(getPath());
            assert cu != null;
            writer.write(cu.toString());
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
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

    private void scanTest() throws FileNotFoundException{
        // The directory where we store the examples
        // Parse the code of an entire source file, a.k.a. a Compilation Unit
        CompilationUnit cu = StaticJavaParser.parse(new File(getPath()));
        ClassOrInterfaceDeclaration hw = cu.getClassByName("hw").get();
        List<MethodDeclaration> x= hw.getMethods();
        for(MethodDeclaration md:x){
            System.out.println(md.getName());
        }
    }

    private static String getPath(){
        return System.getProperty("user.dir") + File.separator + "Mistletoe-java" + File.separator +
                "src" + File.separator + "main" + File.separator + "resources"  + File.separator + "hw.java";
    }

}

