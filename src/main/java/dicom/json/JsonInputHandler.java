package dicom.json;

import org.dcm4che3.data.*;
import org.dcm4che3.io.DicomInputHandler;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.util.Base64;
import org.dcm4che3.util.StringUtils;
import org.dcm4che3.util.TagUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.stream.JsonGenerator;
import java.io.IOException;

public class JsonInputHandler implements DicomInputHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonInputHandler.class);

    private final JsonGenerator jsonGenerator;

    private final boolean includeBinary;

    private boolean sopInstanceUidReady = false;

    private String sopInstanceUid = null;

    private boolean patientIdReady = false;

    private String patientId = null;

    public JsonInputHandler(JsonGenerator jsonGenerator) {

        this(jsonGenerator, false);
    }

    public JsonInputHandler(JsonGenerator jsonGenerator, boolean includeBinary) {

        this.jsonGenerator = jsonGenerator;
        this.includeBinary = includeBinary;
    }

    public String getSopInstanceUid() {

        return sopInstanceUidReady ? sopInstanceUid : null;
    }

    public String getPatientId() {

        return patientIdReady ? patientId : null;
    }

    @Override
    public void readValue(DicomInputStream dis, Attributes attrs) throws IOException {

        VR vr = dis.vr();
        int tag = dis.tag();
        int len = dis.length();

        if (TagUtils.isGroupLength(tag)) {
            dis.readValue(dis, attrs);
        } else if (dis.isExcludeBulkData()) {
            dis.readValue(dis, attrs);
        } else {
            String htag = 't' + TagUtils.toHexString(tag);

            if (vr == VR.SQ || len == -1) {
                jsonGenerator.writeStartArray(htag);
                dis.readValue(dis, attrs);
                jsonGenerator.writeEnd();
            } else if (len > 0) {
                if (dis.isIncludeBulkDataURI()) {
                    writeBulkData(tag, dis.createBulkData(dis));
                } else {
                    byte[] b = dis.readValue();

                    if (tag == Tag.TransferSyntaxUID || tag == Tag.SpecificCharacterSet) {
                        attrs.setBytes(tag, vr, b);
                    }

                    writeValue(tag, vr, b, dis.bigEndian(), attrs.getSpecificCharacterSet(vr), false);
                }
            } else {
                jsonGenerator.writeNull(htag);
            }
        }
    }

    private void writeValue(int tag, VR vr, Object val, boolean bigEndian, SpecificCharacterSet cs, boolean preserve) {
        switch (vr) {
            case AE:
            case AS:
            case AT:
            case CS:
            case DA:
            case DS:
            case DT:
            case IS:
            case LO:
            case LT:
            case PN:
            case SH:
            case ST:
            case TM:
            case UC:
            case UI:
            case UR:
            case UT:
                writeStringValues(tag, vr, val, bigEndian, cs);
                break;
            case FL:
            case FD:
                writeDoubleValues(tag, vr, val, bigEndian);
                break;
            case SL:
            case SS:
            case US:
                writeIntValues(tag, vr, val, bigEndian);
                break;
            case UL:
                writeUIntValues(tag, vr, val, bigEndian);
                break;
            case OB:
            case OD:
            case OF:
            case OL:
            case OW:
            case UN:
                writeInlineBinary(tag, vr, (byte[]) val, bigEndian, preserve);
                break;
            case SQ:
                assert true;
        }
    }

    private void writeStringValues(int tag, VR vr, Object val, boolean bigEndian, SpecificCharacterSet cs) {

        String htag = 't' + TagUtils.toHexString(tag);
        Object o = vr.toStrings(val, bigEndian, cs);

        if (o instanceof String[]) {
            jsonGenerator.writeStartArray(htag);

            for (String s : (String[]) o) {
                if (s == null || s.isEmpty())
                    jsonGenerator.writeNull();
                else switch (vr) {
                    case DS:
                        try {
                            jsonGenerator.write(StringUtils.parseDS(s));
                        } catch (NumberFormatException e) {
                            LOGGER.info("illegal DS value: {} - encoded as null", s);
                            jsonGenerator.writeNull();
                        }
                        break;
                    case IS:
                        try {
                            jsonGenerator.write(StringUtils.parseIS(s));
                        } catch (NumberFormatException e) {
                            LOGGER.info("illegal IS value: {} - encoded as null", s);
                            jsonGenerator.writeNull();
                        }
                        break;
                    case PN:
                        PersonName pn = new PersonName(s, true);
                        jsonGenerator.write(pn.toString());
                        break;
                    default:
                        jsonGenerator.write(s);

                        switch (tag) {
                            case 524312:
                                if (sopInstanceUid == null) {
                                    sopInstanceUid = s;
                                } else {
                                    sopInstanceUid = sopInstanceUid + ';' + s;
                                }
                                break;
                            case 1048608:
                                if (patientId == null) {
                                    patientId = s;
                                } else {
                                    patientId = patientId + ';' + s;
                                }
                                break;
                        }
                }
            }

            jsonGenerator.writeEnd();
        } else {
            String s = (String) o;
            if (s == null || s.isEmpty())
                jsonGenerator.writeNull(htag);
            else switch (vr) {
                case DS:
                    try {
                        jsonGenerator.write(htag, StringUtils.parseDS(s));
                    } catch (NumberFormatException e) {
                        LOGGER.info("illegal DS value: {} - encoded as null", s);
                        jsonGenerator.writeNull(htag);
                    }
                    break;
                case IS:
                    try {
                        jsonGenerator.write(htag, StringUtils.parseIS(s));
                    } catch (NumberFormatException e) {
                        LOGGER.info("illegal IS value: {} - encoded as null", s);
                        jsonGenerator.writeNull(htag);
                    }
                    break;
                case PN:
                    PersonName pn = new PersonName(s, true);
                    jsonGenerator.write(htag, pn.toString());
                    break;
                default:
                    jsonGenerator.write(htag, s);

                    switch (tag) {
                        case 524312:
                            sopInstanceUid = s;
                            break;
                        case 1048608:
                            patientId = s;
                            break;
                    }
            }
        }
    }

    private void writeDoubleValues(int tag, VR vr, Object val, boolean bigEndian) {

        String htag = 't' + TagUtils.toHexString(tag);

        int vm = vr.vmOf(val);
        switch (vm) {
            case 0:
                jsonGenerator.writeNull(htag);
                break;
            case 1:
                jsonGenerator.write(htag, vr.toDouble(val, bigEndian, 0, 0));
                break;
            default:
                jsonGenerator.writeStartArray(htag);

                for (int i = 0; i < vm; i++) {
                    jsonGenerator.write(vr.toDouble(val, bigEndian, i, 0));
                }

                jsonGenerator.writeEnd();
        }
    }

    private void writeIntValues(int tag, VR vr, Object val, boolean bigEndian) {

        String htag = 't' + TagUtils.toHexString(tag);

        int vm = vr.vmOf(val);
        switch (vm) {
            case 0:
                jsonGenerator.writeNull(htag);
                break;
            case 1:
                jsonGenerator.write(htag, vr.toInt(val, bigEndian, 0, 0));
                break;
            default:
                jsonGenerator.writeStartArray(htag);

                for (int i = 0; i < vm; i++) {
                    jsonGenerator.write(vr.toInt(val, bigEndian, i, 0));
                }

                jsonGenerator.writeEnd();
        }
    }

    private void writeUIntValues(int tag, VR vr, Object val, boolean bigEndian) {

        String htag = 't' + TagUtils.toHexString(tag);

        int vm = vr.vmOf(val);
        switch (vm) {
            case 0:
                jsonGenerator.writeNull(htag);
                break;
            case 1:
                jsonGenerator.write(htag, vr.toInt(val, bigEndian, 0, 0) & 0xffffffffL);
                break;
            default:
                jsonGenerator.writeStartArray(htag);

                for (int i = 0; i < vm; i++) {
                    jsonGenerator.write(vr.toInt(val, bigEndian, i, 0) & 0xffffffffL);
                }

                jsonGenerator.writeEnd();
        }
    }

    private void writeBulkData(int tag, BulkData blkdata) {

        if (includeBinary) {
            String htag = 't' + TagUtils.toHexString(tag);

            jsonGenerator.write(htag, blkdata.getURI());
        }
    }

    private void writeInlineBinary(int tag, VR vr, byte[] b, boolean bigEndian, boolean preserve) {

        if (includeBinary) {
            String htag = 't' + TagUtils.toHexString(tag);

            if (bigEndian) {
                b = vr.toggleEndian(b, preserve);
            }

            jsonGenerator.write(htag, encodeBase64(b));
        }
    }

    private String encodeBase64(byte[] b) {

        int len = (b.length * 4 / 3 + 3) & ~3;
        char[] ch = new char[len];

        Base64.encode(b, 0, b.length, ch, 0);

        return new String(ch);
    }

    @Override
    public void readValue(DicomInputStream dis, Sequence seq) throws IOException {

        jsonGenerator.writeStartObject();
        dis.readValue(dis, seq);
        jsonGenerator.writeEnd();
    }

    @Override
    public void readValue(DicomInputStream dis, Fragments frags) throws IOException {

        throw new UnsupportedOperationException("DICOM fragments not supported");
    }

    @Override
    public void startDataset(DicomInputStream dis) throws IOException {

        sopInstanceUid = null;
        sopInstanceUidReady = false;

        patientId = null;
        patientIdReady = false;

        jsonGenerator.writeStartObject();
    }

    @Override
    public void endDataset(DicomInputStream dis) throws IOException {

        jsonGenerator.writeEnd();

        sopInstanceUidReady = true;

        patientIdReady = true;
    }
}
