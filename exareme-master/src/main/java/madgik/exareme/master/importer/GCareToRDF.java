package madgik.exareme.master.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class GCareToRDF {

    private static final String RDFTYPE = " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ";
    private static final String TYPEENTITYPREFIX = " <http://uoa.gr/type/";
    private static final String PROPERTYPREFIX = " <http://uoa.gr/property/";
    private static final String ENTITYPREFIX = " <http://uoa.gr/entity/";
    private static final String CLOSING = "> ";
    private static final String LINEEND = " . ";
    private static final String VARIABLEPREFIX = " ?variable";



    public static void main(String[] args) {
        //arg0 : dara or query
        //arg1: filename
        try {
            File file = new File(args[1]);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            if(args[0].equals("data")) {
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("t"))
                        continue;
                    if (line.startsWith("v")) {
                        //rdf type triples
                        String[] values = line.split(" ");
                        String entity = ENTITYPREFIX + values[1] + CLOSING;
                        for (int i = 2; i < values.length; i++) {
                            System.out.println(entity + RDFTYPE + TYPEENTITYPREFIX + values[i] + CLOSING + LINEEND);
                        }
                    }
                    if (line.startsWith("e")) {
                        String[] values = line.split(" ");
                        System.out.println(ENTITYPREFIX + values[1] +
                                CLOSING + PROPERTYPREFIX + values[3] + CLOSING +
                                ENTITYPREFIX + values[2] +
                                CLOSING + LINEEND);
                    }
                }
            }
            if(args[0].equals("query")) {
                System.out.print("SELECT * WHERE { ");
                Map<String,String> nodesIDtoIRIs = new HashMap<>();
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("t"))
                        continue;
                    if (line.startsWith("v")) {
                        //rdf type triples
                        String[] values = line.split(" ");

                        if(values[3].equals("-1")){
                            if(!values[2].equals("-1")) {
                                System.out.print(VARIABLEPREFIX + values[1] + RDFTYPE + TYPEENTITYPREFIX + values[2] + "> . ");
                            }
                            nodesIDtoIRIs.put(values[1],  VARIABLEPREFIX+values[1]+" ");
                        }
                        else{
                            nodesIDtoIRIs.put(values[1], ENTITYPREFIX+values[1]+"> ");
                        }
                    }
                    if (line.startsWith("e")) {
                        String[] values = line.split(" ");
                        System.out.print(nodesIDtoIRIs.get(values[1]) + PROPERTYPREFIX+ values[3]+"> "+nodesIDtoIRIs.get(values[2]) + " . ");
                    }
                }
                System.out.println(" } ");
            }
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}



