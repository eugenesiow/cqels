import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;


public class test {
	private static final String STREAM_ID_PREFIX = "http://example.org/simpletest/test";
    private static final String CQELS_HOME = "cqels_home";
    private static ExecContext context;

	public static void main(String[] args) {
		File home = new File(CQELS_HOME);
        if (!home.exists()) {
            home.mkdir();
        }
        context = new ExecContext(CQELS_HOME, true);
        simpleSelect();
	}
	
	public static void simpleSelect() {
        final String STREAM_ID = STREAM_ID_PREFIX + "_1";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousSelect query = context.registerSelect(""
                + "SELECT ?x ?y ?z WHERE {"
                + "STREAM <" + STREAM_ID + "> [NOW] {?x ?y ?z}"
                + "}");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        long startTime = System.currentTimeMillis();
        
        stream.stream(new Triple(
                Node.createURI("http://example.org/resource/1"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));

        List<Mapping> mappings = null;
		try {
			mappings = listener.call();
		} catch (Exception e) {
			e.printStackTrace();
		}
        List<Node> nodes = toNodeList(context, mappings.get(0));
        System.out.println(nodes.get(0).getURI());
        System.out.println(nodes.get(1).getURI());
        System.out.println(nodes.get(2).getLiteralValue());
        
        System.out.println(System.currentTimeMillis() - startTime);
    }

	public static List<Node> toNodeList(ExecContext context, Mapping mapping) {
        List<Node> nodes = new ArrayList<Node>();
        for (Iterator<Var> vars = mapping.vars(); vars.hasNext();) {
            final long id = mapping.get(vars.next());
            if (id > 0) {
                nodes.add(context.engine().decode(id));
            } else {
                nodes.add(null);
            }
        }
        return nodes;
    }
}
