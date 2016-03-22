import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.graph.Node;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.vocabulary.RDF;


public class test {
	private static final String STREAM_ID = "http://www.cwi.nl/SRBench/observations";
    private static final String CQELS_HOME = "cqels_home";
    private static ExecContext context;
    private static int MAX_EVENTS = 101;

	public static void main(String[] args) {
		File home = new File(CQELS_HOME);
        if (!home.exists()) {
            home.mkdir();
        }
        context = new ExecContext(CQELS_HOME, true);
        
        
        String fileName = "queries/q4.sparql";
        if (args.length > 0) {
        	fileName = args[0];
        }
        String colMapPath = "colmap/alpha.csv";
        if (args.length > 1) {
        	colMapPath = args[1];
        }
        String folderPath = "/Users/eugene/Downloads/knoesis_observations_ike_csv/";
        if (args.length > 2) {
        	folderPath = args[2];
        }
        String stationName = "ALPHA";
        if (args.length > 3) {
        	stationName = args[3];
        }
        long sleepTime = 1000;
        if (args.length > 4) {
        	sleepTime = Long.parseLong(args[4]);
        }
        if (args.length > 5) {
        	MAX_EVENTS = Integer.parseInt(args[5]);
        }
        String defaultDataset = "colmap/ALPHA_meta.nt";
        if (args.length > 6) {
        	defaultDataset = args[6];
        }
        context.loadDefaultDataset(defaultDataset);
        
        File queryFile = new File(fileName);
        String queryStr = "";
        try {
			queryStr = FileUtils.readFileToString(queryFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        simpleSelect(queryStr,colMapPath,folderPath,stationName,sleepTime);
	}
	
	public static void simpleSelect(String queryStr, String colMapPath, String folderPath, String stationName,long sleepTime) {
        DefaultRDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousSelect query = context.registerSelect(queryStr);
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        Map<String,String> uom = new HashMap<String,String>();
        Map<String,String> classType = new HashMap<String,String>();
        Map<String,String> property = new HashMap<String,String>();
        
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
        
        try {
			BufferedReader br = new BufferedReader(new FileReader(folderPath + stationName + ".csv"));
			String[] header = br.readLine().split(",");
			List<String> headerList = new ArrayList<String>();
			for(int i=1;i<header.length;i++) {
				headerList.add(header[i]);
			}
			String line="";
			int count = 0;
			while((line=br.readLine())!=null && count<MAX_EVENTS) {
				String[] parts = line.split(",");
				Model model = ModelFactory.createDefaultModel();
				Resource instant = model.createResource();
				Resource sensor = model.createResource("http://knoesis.wright.edu/ssw/System_"+stationName);
				
				for(int j=1;j<parts.length;j++) {
					Resource obs = model.createResource();
					Resource result = model.createResource();
					String colHead = headerList.get(j-1);
					String uomStr = uom.get(colHead);
					//add ssn structure
					model.add(obs,RDF.type,model.createResource(classType.get(colHead)));
					model.add(obs,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result"),result);
					model.add(obs,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#procedure"),sensor);
					model.add(sensor,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#generatedObservation"),obs);
					model.add(obs,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#observedProperty"),model.createResource(property.get(colHead)));
					model.add(obs,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#samplingTime"),instant);
					if(!uomStr.equals("bool")) {
						model.add(result,RDF.type,model.createResource("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#MeasureData"));
						model.add(result,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue"),model.createLiteral(parts[j]));
						model.add(result,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#uom"),model.createResource(uomStr));
					} else {
						model.add(result,RDF.type,model.createResource("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#TruthData"));
						model.add(result,model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#booleanValue"),model.createLiteral(parts[j]));
					}
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

				count++;
		        
		        for(Mapping mapping:mappings) {
		        	List<Node> nodes = toNodeList(context, mapping);
//		        	for(Node node:nodes) {
//		        		if(node!=null)
//		        			System.out.println(node.toString());
//		        	}
		        }
		        
		        System.out.println(System.currentTimeMillis() - startTime);
		        Thread.sleep(sleepTime);
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
