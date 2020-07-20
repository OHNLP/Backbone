package org.ohnlp.backbone.core.coders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import java.io.*;
import java.util.Collection;
import java.util.Map;

public class RowToByteArrCoder extends AtomicCoder<Row> {
    private ObjectMapper om = new ObjectMapper();
    private ObjectWriter writer = om.writer();

    @Override
    public void encode(Row value, OutputStream outStream) throws CoderException, IOException {
//        byte[] schema = SerializationUtils.serialize(value.getSchema());
//        ObjectNode obj = toObjectNode(value);
//        obj.put("__backbone_internal_schema", schema);
//        om.writer().writeValue(outStream, obj);
        serialize(value, outStream);
    }

    @Override
    public Row decode(InputStream inStream) throws CoderException, IOException {
        return deserialize(inStream);
    }

    // Clones of Commons SerializationUtils without closing the streams
    private void serialize(Serializable obj, OutputStream outputStream) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(outputStream);
            Throwable var3 = null;

            try {
                out.writeObject(obj);
            } catch (Throwable var13) {
                var3 = var13;
                throw var13;
            }

        } catch (IOException var15) {
            throw new SerializationException(var15);
        }
    }

    // Clones of Commons SerializationUtils without closing the streams
    public static <T> T deserialize(InputStream inputStream) {
        try {
            ObjectInputStream in = new ObjectInputStream(inputStream);
            Throwable var2 = null;

            T var4;
            try {
                T obj = (T) in.readObject();
                var4 = obj;
            } catch (Throwable var14) {
                var2 = var14;
                throw var14;
            }

            return var4;
        } catch (IOException | ClassNotFoundException var16) {
            throw new SerializationException(var16);
        }
    }

    private ObjectNode toObjectNode(Row value) {
        ObjectNode out = JsonNodeFactory.instance.objectNode();
        for (Schema.Field f : value.getSchema().getFields()) {
            switch (f.getType().getTypeName()) {
                case BYTE:
                    out.put(f.getName(), value.getByte(f.getName()));
                    break;
                case INT16:
                    out.put(f.getName(), value.getInt16(f.getName()));
                    break;
                case INT32:
                    out.put(f.getName(), value.getInt32(f.getName()));
                    break;
                case INT64:
                    out.put(f.getName(), value.getInt64(f.getName()));
                    break;
                case DECIMAL:
                    out.put(f.getName(), value.getDecimal(f.getName()));
                    break;
                case FLOAT:
                    out.put(f.getName(), value.getFloat(f.getName()));
                    break;
                case DOUBLE:
                    out.put(f.getName(), value.getDouble(f.getName()));
                    break;
                case STRING:
                    out.put(f.getName(), value.getString(f.getName()));
                    break;
                case DATETIME:
                    throw new UnsupportedOperationException("Cannot encode datetimes yet");
                case BOOLEAN:
                    out.put(f.getName(), value.getBoolean(f.getName()));

                    break;
                case BYTES:
                    out.put(f.getName(), value.getBytes(f.getName()));
                    break;
                case ARRAY:
                    Collection<?> coll = value.getArray(f.getName());
                    out.set(f.getName(), om.valueToTree(coll));
                case ITERABLE:
                    Iterable<?> it = value.getIterable(f.getName());
                    out.set(f.getName(), om.valueToTree(it));
                    break;
                case MAP:
                    Map<?, ?> m = value.getMap(f.getName());
                    out.set(f.getName(), om.valueToTree(m));
                    break;
                case ROW:
                    Row r = value.getRow(f.getName());
                    out.set(f.getName(), toObjectNode(r));
                    break;
                case LOGICAL_TYPE:
                    throw new UnsupportedOperationException("Logical Types are not supported");
            }
        }
        return out;
    }
}
