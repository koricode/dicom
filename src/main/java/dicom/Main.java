package dicom;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dicom.json.JsonInputHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.dcm4che3.io.DicomInputStream;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;
import org.lightcouch.NoDocumentException;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import java.io.*;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;

public class Main {

    private JsonParser jsonParser = new JsonParser();

    private CouchDbClient couchdbClient;

    private Connection hbaseClient;

    private Properties props;

    private File target;

    public Main(String propsPath, String targetPath) throws IOException {

        props = new Properties();

        try (FileInputStream fis = new FileInputStream(propsPath)) {
            props.load(fis);
        }

        target = new File(targetPath);
    }

    public void process() throws IOException {

        if (target.exists()) {
            openHBaseClient();
            openCouchDbClient();


            if (target.isFile()) {
                processFile(target);
            } else {
                processFolder(target);
            }

            closeCouchDbClient();
            closeHBaseClient();
        }
    }

    private void openCouchDbClient() throws IOException {

        if (couchdbClient == null) {
            System.out.println("Conectando ao CouchDB...");

            String protocol = props.getProperty("couchdb.protocol");
            String host = props.getProperty("couchdb.host");
            String port = props.getProperty("couchdb.port");
            String name = props.getProperty("couchdb.name");
            String username = props.getProperty("couchdb.username");
            String password = props.getProperty("couchdb.password");

            CouchDbProperties couchdbConfig = new CouchDbProperties();

            if (protocol != null) {
                protocol = protocol.trim();
                if (!protocol.isEmpty()) {
                    couchdbConfig.setProtocol(protocol);
                }
            }

            if (host != null) {
                host = host.trim();
                if (!host.isEmpty()) {
                    couchdbConfig.setHost(host);
                }
            }

            if (port != null) {
                port = port.trim();
                if (!port.isEmpty()) {
                    try {
                        couchdbConfig.setPort(Integer.parseInt(port));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }

            if (name != null) {
                name = name.trim();
                if (!name.isEmpty()) {
                    couchdbConfig.setDbName(name);
                }
            }

            if (username != null) {
                username = username.trim();
                if (!username.isEmpty()) {
                    couchdbConfig.setUsername(username);
                }
            }

            if (password != null) {
                password = password.trim();
                if (!password.isEmpty()) {
                    couchdbConfig.setPassword(password);
                }
            }

            couchdbClient = new CouchDbClient(couchdbConfig);

            System.out.println("Conectado ao CouchDB!");
        }
    }

    private void closeCouchDbClient() throws IOException {

        if (couchdbClient != null) {
            System.out.println("Desconectando do CouchDB...");

            couchdbClient.shutdown();
            couchdbClient = null;

            System.out.println("Desconectado do CouchDB!");
        }
    }

    private void openHBaseClient() throws IOException {

        if (hbaseClient == null) {
            System.out.println("Conectando ao HBase...");

            String host = props.getProperty("hbase.host");
            String port = props.getProperty("hbase.port");

            Configuration hbaseConfig = HBaseConfiguration.create();

            if (host != null) {
                host = host.trim();
                if (!host.isEmpty()) {
                    hbaseConfig.set("hbase.zookeeper.quorum", host);
                }
            }

            if (port != null) {
                port = port.trim();
                if (!port.isEmpty()) {
                    try {
                        hbaseConfig.setInt("hbase.zookeeper.property.clientPort", Integer.parseInt(port));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }

            hbaseConfig.setInt("hbase.client.keyvalue.maxsize", 0);

            HBaseAdmin.available(hbaseConfig);

            hbaseClient = ConnectionFactory.createConnection(hbaseConfig);

            System.out.println("Conectado ao HBase!");
        }
    }

    private void closeHBaseClient() throws IOException {

        if (hbaseClient != null) {
            System.out.println("Desconectando do HBase...");

            try {
                hbaseClient.close();
                hbaseClient = null;
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Desconectado do HBase!");
        }
    }

    private void processFile(File file) throws IOException {

        if (file.getName().endsWith(".dcm")) {

            byte[] dataBytes = Files.readAllBytes(file.toPath());

            String patientId;
            String sopInstanceId;
            String rev;
            byte[] jsonBytes;
            JsonObject jsonObject;

            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                try (JsonGenerator jsonGenerator = Json.createGeneratorFactory(Collections.emptyMap()).createGenerator(os)) {
                    JsonInputHandler dicomHandler = new JsonInputHandler(jsonGenerator);

                    try (DicomInputStream is = new DicomInputStream(new ByteArrayInputStream(dataBytes))) {
                        is.setIncludeBulkData(DicomInputStream.IncludeBulkData.NO);
                        is.setDicomInputHandler(dicomHandler);
                        is.readDataset(-1, -1);
                    }

                    sopInstanceId = dicomHandler.getSopInstanceUid();
                    patientId = dicomHandler.getPatientId();
                }

                jsonBytes = os.toByteArray();
                jsonObject = (JsonObject) jsonParser.parse(new String(jsonBytes));
            }

            try {
                rev = couchdbClient.find(JsonObject.class, sopInstanceId).get("_rev").getAsString();
            } catch (NoDocumentException e) {
                rev = null;
            }

            if (rev == null) {
                jsonObject.addProperty("_id", sopInstanceId);
                couchdbClient.save(jsonObject);
            } else {
                jsonObject.addProperty("_id", sopInstanceId);
                jsonObject.addProperty("_rev", rev);
                couchdbClient.update(jsonObject);
            }

            Table table = hbaseClient.getTable(TableName.valueOf("dicom"));

            Put put = new Put(Bytes.toBytes(patientId));

            put.addColumn(Bytes.toBytes("file"), Bytes.toBytes(sopInstanceId), dataBytes);

            table.put(put);

            System.out.println(String.format("Dados inseridos ( %s ) | Arquivo %s", sopInstanceId, file.toString()));
        }
    }

    private void processFolder(File folder) throws IOException {

        for (File target : folder.listFiles()) {
            if (target.isFile()) {
                processFile(target);
            } else {
                processFolder(target);
            }
        }
    }

    public static void main(String... args) throws IOException {

        switch (args.length) {
            case 1:
                new Main("./config.properties", args[0]).process();
                break;
            case 2:
                new Main(args[0], args[1]).process();
                break;
            default:
                System.out.println("Informe o caminho para a configuração e para o diretório contendo os arquivos DICOM");
        }
    }
}
