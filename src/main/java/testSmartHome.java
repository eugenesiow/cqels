import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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


public class testSmartHome {
	private static final String STREAM_ID = "http://iot.soton.ac.uk/smarthome/observations";
    private static final String CQELS_HOME = "cqels_home";
    private static ExecContext context;
    private static int MAX_EVENTS = 101;

	public static void main(String[] args) {
		File home = new File(CQELS_HOME);
        if (!home.exists()) {
            home.mkdir();
        }
        context = new ExecContext(CQELS_HOME, true);
        
        
        String fileName = "queries/smarthome/q3.sparql";
        if (args.length > 0) {
        	fileName = args[0];
        }
        String envPath = "/Users/eugene/Downloads/homeA-all/homeA-environmental/all-environmental-sort.csv";
        if (args.length > 1) {
        	envPath = args[1];
        }
        String meterPath = "/Users/eugene/Downloads/homeA-all/homeA-meter/all-meter-replace.csv";
        if (args.length > 2) {
        	meterPath = args[2];
        }
        String motionPath = "/Users/eugene/Downloads/homeA-all/homeA-motion/all-motion-replace.csv";
        if (args.length > 3) {
        	motionPath = args[3];
        }
        long sleepTime = 0;
        if (args.length > 4) {
        	sleepTime = Long.parseLong(args[4]);
        }
        if (args.length > 5) {
        	MAX_EVENTS = Integer.parseInt(args[5]);
        }
        Boolean printCount = false;
        if(args.length > 6) {
        	printCount=Boolean.parseBoolean(args[6]);
        }
        
        File queryFile = new File(fileName);
        String queryStr = "";
        try {
			queryStr = FileUtils.readFileToString(queryFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        simpleSelect(queryStr,envPath,meterPath,motionPath,sleepTime,printCount);
	}
	
	public static void simpleSelect(String queryStr, String envPath, String meterPath, String motionPath,long sleepTime,Boolean printCount) {
        DefaultRDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousSelect query = context.registerSelect(queryStr);
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);
       
        try {
			BufferedReader br1 = new BufferedReader(new FileReader(envPath));
			BufferedReader br2 = new BufferedReader(new FileReader(meterPath));
			BufferedReader br3 = new BufferedReader(new FileReader(motionPath));
			String line1,line2,line3 = "";		
			Map<String,Resource> meters = new HashMap<String,Resource>();
			Map<String,Resource> motions = new HashMap<String,Resource>();
			
        	for(int i=0;i<MAX_EVENTS;i++) {
        		Model model = ModelFactory.createDefaultModel();        		
        		line1=br1.readLine();
        		String[] line1Parts = line1.split(",");
        		
        		Resource sensor = model.createResource("http://iot.soton.ac.uk/smarthome/sensor#environmental1");
        		Resource obs = model.createResource();
        		Resource result = model.createResource();
        		
        		model.add(obs,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#WeatherObservation"));        		
				model.add(obs,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#observationResult"),result);
				model.add(obs,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#observedBy"),sensor);
				
				Resource value1 = model.createResource();
        		Resource value2 = model.createResource();
				model.add(result,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#WeatherSensorOutput"));        		
				model.add(result,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#hasValue"),value1);
				model.add(result,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#hasValue"),value2);
				model.add(result,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#isProducedBy"),sensor);
				
				model.add(value1,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#InternalTemperatureValue"));        		
				model.add(value1,model.createProperty("http://purl.oclc.org/NET/iot#hasQuantityValue"),model.createLiteral(line1Parts[1].trim()));
				model.add(value2,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#ExternalTemperatureValue"));        		
				model.add(value2,model.createProperty("http://purl.oclc.org/NET/iot#hasQuantityValue"),model.createLiteral(line1Parts[2].trim()));
		
        		line2=br2.readLine();
				String[] line2Parts = line2.split(",");
				
				String meterName = line2Parts[0].trim().replace(":", "_");
				
				Resource sensormeter = meters.get(meterName);
				if(sensormeter==null) {
					sensormeter = model.createResource("http://iot.soton.ac.uk/smarthome/sensor#"+meterName);
					meters.put(meterName, sensormeter);
				}
        		Resource obsMeter = model.createResource();
        		Resource resultMeter = model.createResource();
        		
        		model.add(obsMeter,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#EnergyObservation"));        		
				model.add(obsMeter,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#observationResult"),resultMeter);
				model.add(obsMeter,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#observedBy"),sensormeter);
				
				Resource valueMeter = model.createResource();
				model.add(resultMeter,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#EnergySensorOutput"));        		
				model.add(resultMeter,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#hasValue"),valueMeter);
				model.add(resultMeter,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#isProducedBy"),sensormeter);
				
				model.add(valueMeter,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#EnergyValue"));        		
				model.add(valueMeter,model.createProperty("http://purl.oclc.org/NET/iot#hasQuantityValue"),model.createLiteral(line2Parts[2].trim()));
				
        		line3=br3.readLine();
				
				String[] line3Parts = line3.split(",");
				
				String motionName = line3Parts[0].trim().replace(":corner", "_motion");
				
				Resource sensormotion = motions.get(motionName);
				if(sensormotion==null) {
					sensormotion = model.createResource("http://iot.soton.ac.uk/smarthome/sensor#"+motionName);
					motions.put(motionName, sensormotion);
				}
        		Resource obsMotion = model.createResource();
        		Resource resultMotion = model.createResource();
        		
        		model.add(obsMotion,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#MotionObservation"));        		
				model.add(obsMotion,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#observationResult"),resultMotion);
				model.add(obsMotion,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#observedBy"),sensormotion);
				
				Resource valueMotion = model.createResource();
				model.add(resultMotion,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#MotionSensorOutput"));        		
				model.add(resultMotion,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#hasValue"),valueMotion);
				model.add(resultMotion,model.createProperty("http://purl.oclc.org/NET/ssnx/ssn#isProducedBy"),sensormotion);
				
				model.add(valueMotion,RDF.type,model.createResource("http://purl.oclc.org/NET/iot#MotionValue"));        		
				model.add(valueMotion,model.createProperty("http://purl.oclc.org/NET/iot#hasQuantityValue"),model.createLiteral(line3Parts[2].trim()));
        		
				long startTime = System.currentTimeMillis();
		        stream.stream(model);

		        List<Mapping> mappings = null;
				try {
					mappings = listener.call();
					model.close();
				} catch (Exception e) {
					e.printStackTrace();
				}

				int itemcount = 0;
		        for(Mapping mapping:mappings) {
		        	if(printCount) {
			        	List<Node> nodes = toNodeList(context, mapping);
			        	for(Node node:nodes) {
			        		if(node!=null) {
			        			itemcount++;
//			        			System.out.println(node.toString());
			        		}
			        	}
		        	}
		        }
		        
		        if(printCount) {
		        	System.out.println(System.currentTimeMillis() - startTime + ":"+itemcount);
		        } else {
		        	System.out.println(System.currentTimeMillis() - startTime);
		        }
		        Thread.sleep(sleepTime);
        	}
        	br1.close();
        	br2.close();
        	br3.close();
			System.exit(0);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
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
