import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OElement;

public class TestBrowse {
    public void test(ODatabaseSession db) {
        for (OElement e : db.browseClass("MyClass", false)) {
            System.out.println(e);
        }
    }
}
