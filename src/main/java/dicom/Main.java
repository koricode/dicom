package dicom;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dicom.json.JsonInputHandler;
import org.dcm4che3.io.DicomInputStream;
import org.postgresql.util.PGobject;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Main {

    public static final String[] KEY_TAGS = new String[] {
            "t00100020",
            "t0020000E",
            "t0020000D",
            "t00080018"
    };

    public static final String[] IDX_TAGS = new String[] {
            "t00080020",
            "t00080030",
            "t00081030",
            "t0008103E",
            "t00080060",
            "t00100010",
            "t00100030"
    };

    private JsonParser jsonParser = new JsonParser();

    private File target;

    private Connection postgresConnection;

    private Session cassandraSession;

    public Main(File target, Connection postgresConnection, Session cassandraSession) {

        this.target = target;
        this.postgresConnection = postgresConnection;
        this.cassandraSession = cassandraSession;
    }

    public void process() throws IOException, SQLException {

        if (target.exists()) {
            if (target.isFile()) {
                processFile(target);
            } else {
                processFolder(target);
            }
        }
    }

    private void processFile(File file) throws IOException, SQLException {

        if (file.getName().endsWith(".dcm")) {
            StringWriter json = new StringWriter();

            try (JsonGenerator jsonGenerator = Json.createGeneratorFactory(Collections.emptyMap()).createGenerator(json)) {
                try (DicomInputStream is = new DicomInputStream(new FileInputStream(file))) {
                    is.setIncludeBulkData(DicomInputStream.IncludeBulkData.NO);
                    is.setDicomInputHandler(new JsonInputHandler(jsonGenerator));
                    is.readDataset(-1, -1);
                }
            }

            JsonObject jsonObject = (JsonObject) jsonParser.parse(json.toString());

            insertDicomImage(jsonObject, file);
            insertDicomIndex(jsonObject);
            insertDicomTags(jsonObject);

            System.out.println("Processed file " + file.toString());
        }
    }

    private void insertDicomImage(JsonObject jsonObject, File file) throws SQLException, IOException {

        List<Object> values = new ArrayList<>();

        for (String tag : KEY_TAGS) {
            JsonElement el = jsonObject.get(tag);

            if (el instanceof JsonNull || el == null) {
                values.add(null);
            } else {
                values.add(el.getAsString());
            }
        }

        values.add(ByteBuffer.wrap(Files.readAllBytes(file.toPath())));

        cassandraSession.execute("INSERT INTO dicom_image (t00100020,t0020000e,t0020000d,t00080018,data) VALUES (?,?,?,?,?)", values.toArray());
    }

    private void insertDicomIndex(JsonObject jsonObject) throws SQLException {

        StringBuilder whereClause = new StringBuilder();

        for (String tag : KEY_TAGS) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }

            whereClause.append(tag).append(" = ?");
        }

        String id = null;
        try (PreparedStatement stmt = postgresConnection.prepareStatement("SELECT id FROM dicom_index WHERE " + whereClause)) {
            setPostgresStmtParameters(jsonObject, stmt, KEY_TAGS, 1);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    id = rs.getString(1);
                }
            }
        }

        if (id == null) {
            List<String> tags = new ArrayList<>(Arrays.asList(KEY_TAGS));

            StringBuilder insertNamesClause = new StringBuilder("t00100020,t0020000E,t0020000D,t00080018");
            StringBuilder insertValuesClause = new StringBuilder("?,?,?,?");

            for (String tag : IDX_TAGS) {
                JsonElement el = jsonObject.get(tag);

                if (!(el instanceof JsonNull || el == null)) {
                    insertNamesClause.append(',').append(tag);
                    insertValuesClause.append(',').append('?');

                    tags.add(tag);
                }
            }

            try (PreparedStatement stmt = postgresConnection.prepareStatement("INSERT INTO dicom_index (" + insertNamesClause + ") VALUES (" + insertValuesClause + ")")) {
                setPostgresStmtParameters(jsonObject, stmt, tags.toArray(new String[0]), 1);

                stmt.executeUpdate();
            }
        } else {
            List<String> tags = new ArrayList<>();

            StringBuilder updateClause = new StringBuilder();

            for (String tag : IDX_TAGS) {
                JsonElement el = jsonObject.get(tag);

                if (!(el instanceof JsonNull || el == null)) {
                    if (updateClause.length() > 0) {
                        updateClause.append(", ");
                    }

                    updateClause.append(tag).append(" = ?");

                    tags.add(tag);
                }
            }

            try (PreparedStatement stmt = postgresConnection.prepareStatement("UPDATE dicom_index SET " + updateClause + " WHERE id = ?")) {
                setPostgresStmtParameters(jsonObject, stmt, tags.toArray(new String[0]), 1);

                stmt.setString(tags.size() + 1, id);

                stmt.executeUpdate();
            }
        }
    }

    private void insertDicomTags(JsonObject jsonObject) throws SQLException {

        StringBuilder whereClause = new StringBuilder();

        for (String tag : KEY_TAGS) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }

            whereClause.append(tag).append(" = ?");
        }

        boolean exists = false;
        try (PreparedStatement stmt = postgresConnection.prepareStatement("SELECT 1 FROM dicom_tags WHERE " + whereClause)) {
            setPostgresStmtParameters(jsonObject, stmt, KEY_TAGS, 1);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    exists = true;
                }
            }
        }

        if (exists) {
            try (PreparedStatement stmt = postgresConnection.prepareStatement("UPDATE dicom_tags SET tags = ? WHERE " + whereClause)) {
                PGobject pgObject = new PGobject();

                pgObject.setType("json");
                pgObject.setValue(jsonObject.toString());

                stmt.setObject(1, pgObject);

                setPostgresStmtParameters(jsonObject, stmt, KEY_TAGS, 2);

                stmt.executeUpdate();
            }
        } else {
            try (PreparedStatement stmt = postgresConnection.prepareStatement("INSERT INTO dicom_tags VALUES (?,?,?,?,?)")) {
                setPostgresStmtParameters(jsonObject, stmt, KEY_TAGS, 1);

                PGobject pgObject = new PGobject();

                pgObject.setType("json");
                pgObject.setValue(jsonObject.toString());

                stmt.setObject(5, pgObject);

                stmt.executeUpdate();
            }
        }
    }

    private void setPostgresStmtParameters(JsonObject jsonObject, PreparedStatement stmt, String[] tags, int i) throws SQLException {

        for (String tag : tags) {
            JsonElement el = jsonObject.get(tag);

            if (el instanceof JsonNull || el == null) {
                stmt.setNull(i++, Types.VARCHAR);
            } else {
                stmt.setString(i++, el.getAsString());
            }
        }
    }

    private void processFolder(File folder) throws IOException, SQLException {

        for (File target : folder.listFiles()) {
            if (target.isFile()) {
                processFile(target);
            } else {
                processFolder(target);
            }
        }
    }

    public static void main(String... args) throws IOException, SQLException {

        File target;

        Properties props = new Properties();

        switch (args.length) {
            case 1:
                try (FileReader is = new FileReader("./config.properties")) {
                    props.load(is);
                }
                target = new File(args[0]);
                break;
            case 2:
                try (FileReader is = new FileReader(args[0])) {
                    props.load(is);
                }
                target = new File(args[1]);
                break;
            default:
                System.out.println("Informe o caminho para a configuração e/ou para o diretório contendo os arquivos DICOM");
                return;
        }

        try (Connection postgresConnection = DriverManager.getConnection(props.getProperty("pgsql.url"), props.getProperty("pgsql.username"), props.getProperty("pgsql.password"))) {
            try (Cluster cassandraCluster = Cluster.builder().addContactPoint(props.getProperty("cassandra.host")).withPort(Integer.parseInt(props.getProperty("cassandra.port"))).build()) {
                try (Session cassandraSession = cassandraCluster.connect("dicom")) {
                    Main main = new Main(target, postgresConnection, cassandraSession);
                    main.process();
                }
            }
        }
    }
}
