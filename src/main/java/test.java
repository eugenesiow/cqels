import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.vocabulary.RDF;


public class test {
	private static final String STREAM_ID = "http://www.cwi.nl/SRBench/observations";
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
        DefaultRDFStream stream = new DefaultRDFStream(context, STREAM_ID);

//        ContinuousSelect query = context.registerSelect("PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#>\n" + 
//        		"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#>\n" + 
//        		"\n" + 
//        		"SELECT DISTINCT ?sensor ?value ?uom\n" + 
//        		"WHERE {\n" + 
//        		"  STREAM <http://www.cwi.nl/SRBench/observations> [RANGE 3600s]\n" + 
//        		"            \n" + 
//        		"  {?observation om-owl:procedure ?sensor ;\n" + 
//        		"               a weather:TemperatureObservation ;\n" + 
//        		"               om-owl:result ?result. "
//        		+ "?result om-owl:floatValue ?value;"
//        		+ "om-owl:uom ?uom}\n"+
//        		"}");
        ContinuousSelect query = context.registerSelect("PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#>\n" + 
        		"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#>\n" + 
        		"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
        		"\n" + 
        		"SELECT ?sensor MIN(?value)\n" + 
        		"WHERE {\n" + 
        		"STREAM <http://www.cwi.nl/SRBench/observations> [RANGE 10800s SLIDE 600s]\n" + 
        		"  {?observation om-owl:procedure ?sensor ;\n" + 
        		"               om-owl:observedProperty weather:_WindSpeed ;\n" + 
        		"               om-owl:result [ om-owl:floatValue ?value ] .}\n" + 
        		"}              \n" + 
        		"GROUP BY ?sensor\n" +
        		"HAVING ( MIN(?value) >= \"0.0\" ) #milesPerHour");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        Map<String,String> uom = new HashMap<String,String>();
        Map<String,String> classType = new HashMap<String,String>();
        Map<String,String> property = new HashMap<String,String>();
        
        String colMapPath = "colmap/rdb2rdf.csv";
        try {
	        BufferedReader br = new BufferedReader(new FileReader(colMapPath));
	        String line="";
	        while((line=br.readLine())!=null) {
	        	String[] parts = line.split(",");
	        	classType.put(parts[0], parts[1]);
	        	property.put(parts[0], parts[2]);
	        	uom.put(parts[0], parts[3]);
	        }
	        br.close();
        }
        catch(IOException e) {
        	e.printStackTrace();
        }
        
        String folderPath = "/Users/eugene/Downloads/knoesis_observations_ike_csv/";
        String stationName = "AIRGL";
        try {
			BufferedReader br = new BufferedReader(new FileReader(folderPath + stationName + ".csv"));
			String[] header = br.readLine().split(",");
			List<String> headerList = new ArrayList<String>();
			for(int i=1;i<header.length;i++) {
				headerList.add(header[i]);
			}
			String line="";
			while((line=br.readLine())!=null) {
				String[] parts = line.split(",");
				Model model = ModelFactory.createDefaultModel();
				Resource instant = model.createResource();
				Resource sensor = model.createResource("http://knoesis.wright.edu/ssw/System_"+stationName);
				
				for(int j=1;j<parts.length;j++) {
					Resource obs = model.createResource();
					Resource result = model.createResource();
					String colHead = headerList.get(j-1);
					//add ssn structure
					model.add(obs,RDF.type,model.createResource(classType.get(colHead)));
					model.add(obs,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result"),result);
					model.add(obs,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#procedure"),sensor);
					model.add(sensor,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#generatedObservation"),obs);
					model.add(obs,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#observedProperty"),model.createResource(property.get(colHead)));
					model.add(obs,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#samplingTime"),instant);
					model.add(result,RDF.type,model.createResource("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#MeasureData"));
					model.add(result,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue"),model.createLiteral(parts[j]));
					model.add(result,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#uom"),model.createResource(uom.get(colHead)));
					model.add(instant,RDF.type,model.createResource("http://www.w3.org/2006/time#Instant"));
					model.add(instant,model.createProperty("http://www.w3.org/2006/time#inXSDDateTime"),model.createLiteral("_"+stationName+".time"));
				}
				
				long startTime = System.currentTimeMillis();
		        stream.stream(model);

		        List<Mapping> mappings = null;
				try {
					mappings = listener.call();
					model.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
		        
		        for(Mapping mapping:mappings) {
		        	List<Node> nodes = toNodeList(context, mapping);
		        	for(Node node:nodes) {
		        		System.out.println(node.toString());
		        	}
		        }
		        
		        System.out.println(System.currentTimeMillis() - startTime);
		        Thread.sleep(1000);
			}
			
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

	public static List<Node> toNodeList(ExecContext context, Mapping mapping) {
        List<Node> nodes = new ArrayList<Node>();
        for (Iterator<Var> vars = mapping.vars(); vars.hasNext();) {
    		Var v = vars.next();
		
            final long id = mapping.get(v);
            if (id > 0) {
                nodes.add(context.engine().decode(id));
            } else {
                nodes.add(null);
            }
		
        }
        return nodes;
    }
}
