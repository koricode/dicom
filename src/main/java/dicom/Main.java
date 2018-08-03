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
import org.lightcouch.NoDocumentException;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

public class Main {

    private static final byte[] QUALIFIER = Bytes.toBytes("");

    private JsonParser jsonParser = new JsonParser();

    private CouchDbClient couchdbClient;

    private Connection hbaseClient;

    private File target;

    public Main(String path) {

        target = new File(path);
    }

    public void process() throws IOException {

        if (target.exists()) {
            Configuration configuration = HBaseConfiguration.create();
            configuration.addResource(Main.class.getClassLoader().getResource("hbase.xml"));

            HBaseAdmin.available(configuration);

            couchdbClient = new CouchDbClient();

            try (Connection connection = ConnectionFactory.createConnection(configuration)) {
                hbaseClient = connection;

                if (target.isFile()) {
                    processFile(target);
                } else {
                    processFolder(target);
                }
            }

            couchdbClient.shutdown();
        }
    }

    private void processFile(File file) throws IOException {

        if (file.getName().endsWith(".dcm")) {

            byte[] dataBytes = Files.readAllBytes(file.toPath());

            String id;
            String rev;
            byte[] jsonBytes;
            JsonObject jsonObject;

            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                try (JsonGenerator jsonGenerator = Json.createGeneratorFactory(Collections.emptyMap()).createGenerator(os)) {
                    JsonInputHandler dicomHandler = new JsonInputHandler(jsonGenerator);

                    try (DicomInputStream is = new DicomInputStream(new ByteArrayInputStream(dataBytes))) {
                        is.setDicomInputHandler(dicomHandler);
                        is.readDataset(-1, -1);
                    }

                    id = dicomHandler.getSopInstanceUid();
                }

                jsonBytes = os.toByteArray();
                jsonObject = (JsonObject) jsonParser.parse(new String(jsonBytes));
            }

            try {
                rev = couchdbClient.find(JsonObject.class, id).get("_rev").getAsString();
            } catch (NoDocumentException e) {
                rev = null;
            }

            if (rev == null) {
                jsonObject.addProperty("_id", id);
                couchdbClient.save(jsonObject);
            } else {
                jsonObject.addProperty("_id", id);
                jsonObject.addProperty("_rev", rev);
                couchdbClient.update(jsonObject);
            }

            Table table = hbaseClient.getTable(TableName.valueOf("dicom"));

            Put put = new Put(Bytes.toBytes(id));

            put.addColumn(Bytes.toBytes("data"), QUALIFIER, dataBytes);
            put.addColumn(Bytes.toBytes("json"), QUALIFIER, jsonBytes);

            table.put(put);
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

        if (args.length == 1) {
            new Main(args[0]).process();
        } else {
            System.out.println("Informe o caminho para o diretório contendo os arquivos DICOM");
        }
    }
}
